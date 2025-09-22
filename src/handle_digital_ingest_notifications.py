#!/usr/bin/env python3

import json
import logging
import traceback
import uuid
from os import getenv

import boto3
from requests import Session
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from urllib3 import Retry

logger = logging.getLogger()
logger.setLevel(logging.INFO)


full_config_path = f"/{getenv('ENV')}/{getenv('APP_CONFIG_PATH')}"
NEXT_SERVICE_MAP = {
    'digital_ingest_discovery': 'digital_ingest_assembly',
    'digital_ingest_webhook': 'digital_ingest_transformation'
}
zodiac_client = Session()
retries = Retry(total=3,
                backoff_factor=0.3,
                status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries)
zodiac_client.mount('http://', adapter)
zodiac_client.mount('https://', adapter)


def get_config(ssm_parameter_path):
    """Fetch config values from Parameter Store.

    Args:
        ssm_parameter_path (str): Path to parameters

    Returns:
        configuration (dict): all parameters found at the supplied path.
    """
    configuration = {}
    try:
        ssm_client = boto3.client(
            'ssm',
            region_name=getenv('AWS_DEFAULT_REGION', 'us-east-1'))

        param_details = ssm_client.get_parameters_by_path(
            Path=ssm_parameter_path,
            Recursive=False,
            WithDecryption=True)

        for param in param_details.get('Parameters', []):
            param_path_array = param.get('Name').split("/")
            section_position = len(param_path_array) - 1
            section_name = param_path_array[section_position]
            configuration[section_name] = param.get('Value')

    except BaseException:
        print("Encountered an error loading config from SSM.")
        traceback.print_exc()
    finally:
        return configuration


def update_package(config, package_id, raw_package_data=None):
    package_data = {"identifier": package_id}
    if raw_package_data:
        package_data.update(raw_package_data)
    try:
        send_http_request(
            f'{config["ZODIAC_BASEURL"].rstrip("/")}/packages/{package_id}/',
            'patch',
            package_data)
    except HTTPError:
        send_http_request(
            f'{config["ZODIAC_BASEURL"].rstrip("/")}/packages/',
            'post',
            package_data)


def construct_event_id():
    return str(uuid.uuid4())


def update_events(config, package_id, service, outcome, message, traceback):
    event_data = {
        'outcome': outcome,
        'service': service,
        'package_identifier': package_id,
        'message': message,
        'traceback': traceback,
        'identifier': construct_event_id()
    }
    send_http_request(
        f'{config["ZODIAC_BASEURL"].rstrip("/")}/events/',
        'post',
        event_data)


def send_http_request(url, method, data=None):
    """Sends HTTP request and checks to ensure completion."""
    logger.info(f"Sending {method} request to {url} with data {data}")
    if data:
        resp = getattr(zodiac_client, method)(url, json=data)
    else:
        resp = getattr(zodiac_client, method)(url)
    try:
        resp.raise_for_status()
        return resp.json()
    except HTTPError as err:
        logging.error(err.response.text)
        # TODO what should happen here? Send a message?
        raise


def send_next_service_message(current_service, package_id, config):
    """Sends message to start next service if applicable."""
    try:
        next_service = NEXT_SERVICE_MAP[current_service]
        logger.info(f"Starting service {next_service}")
        client = boto3.client(
            'sns',
            region_name=getenv('AWS_DEFAULT_REGION', 'us-east-1'))
        client.publish(
            TopicArn=config['SNS_TOPIC'],
            MessageGroupId=f'digital_ingest_notifications-{package_id}',
            Message=f'Start service {next_service} for package {package_id}',
            MessageAttributes={
                'package_id': {
                    'DataType': 'String',
                    'StringValue': package_id,
                },
                'requested_status': {
                    'DataType': 'String',
                    'StringValue': 'START'
                },
                'service': {
                    'DataType': 'String',
                    'StringValue': next_service,
                }
            })
        logger.info(
            f'Message to start service {next_service} for package {package_id} sent.')
    except KeyError:
        logger.info(f'No next service found for {current_service}')
        pass


def lambda_handler(event, context):
    """Main handler for function."""
    logger.info("Message batch received.")

    config = get_config(full_config_path)
    for record in event['Records']:
        try:
            parsed_body = json.loads(record['body'])
        except json.decoder.JSONDecodeError:
            parsed_body = record['body']

        attributes = record['messageAttributes']
        package_id = attributes.get('package_id', {}).get('stringValue')
        service = attributes.get('service', {}).get('stringValue')
        outcome = attributes.get('outcome', {}).get('stringValue')
        message = attributes.get('message', {}).get('stringValue')

        package_data = parsed_body if outcome == 'SUCCESS' else None
        traceback = parsed_body if outcome == 'FAILURE' else None

        if not all([package_id, service, outcome]):
            logging.error(
                f'Unable to find required values in attributes: {attributes}')
            continue

        update_package(config, package_id, package_data)
        update_events(
            config,
            package_id,
            service,
            outcome,
            message,
            traceback)

        if outcome == 'SUCCESS':
            send_next_service_message(service, package_id, config)
