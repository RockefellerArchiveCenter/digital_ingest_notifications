#!/usr/bin/env python3

import json
import logging
import traceback
import uuid
from os import getenv

import boto3
from requests import Session
from requests.exceptions import HTTPError

logger = logging.getLogger()
logger.setLevel(logging.INFO)


full_config_path = f"/{getenv('ENV')}/{getenv('APP_CONFIG_PATH')}"
NEXT_SERVICE_MAP = {
    'digital_ingest_discovery': 'digital_ingest_assembly',
    'digital_ingest_webhook': 'digital_ingest_transformation'
}
zodiac_client = Session()


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
        package_data.update(json.loads(raw_package_data))
    try:
        send_http_request(
            f'{config["ZODIAC_BASEURL"].rstrip("/")}/packages/{package_id}/',
            'put',
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
        'package': package_id,
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
    resp.raise_for_status()
    return resp.json()


def matching_events(package_id, service_name, baseurl,
                    outcome=None, message=None):
    """Returns list of events matching package and service."""
    try:
        package_events = send_http_request(
            f'{baseurl}/packages/{package_id}/events/', 'get')
        return [e for e in package_events if all([
            e['service'] == service_name,
            e['outcome'] == outcome,
            e.get('message') == message])]
    except HTTPError as e:
        if e.response.status_code == 404:
            return []
        else:
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
        logger.debug(record)
        attributes = record['messageAttributes']

        package_id = attributes.get('package_id', {}).get('stringValue')
        package_data = attributes.get('package_data', {}).get('stringValue')
        service = attributes.get('service', {}).get('stringValue')
        outcome = attributes.get('outcome', {}).get('stringValue')
        message = attributes.get('message', {}).get('stringValue')
        traceback = attributes.get('traceback', {}).get('stringValue')

        if not all([package_id, service, outcome]):
            logging.error(
                f'Unable to find required values in attributes: {attributes}')
            continue

        if len(matching_events(
            package_id,
            service,
            config['ZODIAC_BASEURL'].rstrip("/"),
            outcome,
            message
        )) == 0:
            update_package(config, package_id, package_data)
            update_events(
                config,
                package_id,
                service,
                outcome,
                message,
                traceback)

            if outcome == 'SUCCESS':
                send_next_service_message(
                    service,
                    package_id,
                    config)

        else:
            logger.info('Duplicate event found')
