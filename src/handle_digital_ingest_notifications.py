#!/usr/bin/env python3

import json
import logging
import traceback
import uuid
from os import environ

import boto3
from aws_assume_role_lib import assume_role
from requests import Session

logger = logging.getLogger()
logger.setLevel(logging.INFO)


full_config_path = f"/{environ.get('ENV')}/{environ.get('APP_CONFIG_PATH')}"
NEXT_SERVICE_MAP = {
    'ursa_major': 'fornax',
    'webhook': 'aquarius'
}
zodiac_client = Session()


def get_client_with_role(resource):
    """Gets Boto3 client which authenticates with a specific IAM role."""
    session = boto3.Session()
    assumed_role_session = assume_role(session, environ.get('AWS_ROLE_ARN'))
    return assumed_role_session.client(resource)


def get_config(ssm_parameter_path):
    """Fetch config values from Parameter Store.

    Args:
        ssm_parameter_path (str): Path to parameters

    Returns:
        configuration (dict): all parameters found at the supplied path.
    """
    configuration = {}
    try:
        ssm_client = get_client_with_role('ssm')

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


def update_package(attributes, config):
    package_data = {
        'package_id': attributes['package_id']['Value'],
    }
    if attributes.get('package_data'):
        package_data['package_data'] = json.loads(
            attributes['package_data']['Value'])
    send_http_request(
        f'{config["ZODIAC_BASEURL"].rstrip("/")}/packages',
        'post',
        package_data)


def construct_event_id():
    return str(uuid.uuid4())


def update_events(attributes, config):
    package_events = matching_events(
        attributes['package_id'],
        attributes['service'],
        config['ZODIAC_BASEURL'].rstrip("/"))
    logger.debug(package_events)
    if len(package_events) <= 1:
        event_data = {
            'outcome': attributes['outcome']['Value'],
            'service': attributes['service']['Value'],
            'package': attributes['package_id']['Value']
        }
        event_data['identifier'] = package_events[0]['identifier'] if len(
            package_events) == 1 else construct_event_id()
        send_http_request(
            f'{config["ZODIAC_BASEURL"].rstrip("/")}/events',
            'post',
            event_data)
    else:
        raise Exception(
            f'Got more than one matching event for package {attributes["package_id"]}, found {len(package_events)}')


def send_http_request(url, method, data):
    """Sends HTTP request and checks to ensure completion."""
    logger.debug(f"Sending {method} request to {url} with data {data}")
    if data:
        resp = getattr(zodiac_client, method)(url, json=data)
    else:
        resp = getattr(zodiac_client, method)(url)
    resp.raise_for_status()


def matching_events(package_id, service_name, baseurl, outcome=None):
    """Returns list of events matching package and service."""
    package_events = send_http_request(
        f'{baseurl}/packages/{package_id}/events', 'get')
    if outcome:
        return [e for e in package_events if (
            e['service'] == service_name and e['outcome'] == outcome)]
    else:
        return [e for e in package_events if e['service'] == service_name]


def send_next_service_message(current_service, package_id, config):
    """Sends message to start next service if applicable."""
    try:
        next_service = NEXT_SERVICE_MAP[current_service]
        logger.info(f"Starting service {next_service}")
        client = get_client_with_role('sns')
        client.publish(
            TopicArn=config['SERVICE_START_SNS_TOPIC'],
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
        logging.info(
            f'Message to start service {next_service} for package {package_id} sent.')
    except KeyError:
        logging.info(f'No next service found for {current_service}')
        pass


def lambda_handler(event, context):
    """Main handler for function."""
    logger.info("Message received.")

    config = get_config(full_config_path)

    attributes = event['Records'][0]['Sns']['MessageAttributes']
    logger.debug(attributes)

    if len(matching_events(
        attributes['package_id']['Value'],
        attributes['service']['Value'],
        config['ZODIAC_BASEURL'].rstrip("/"),
        attributes['outcome']['Value']
    )) == 0:
        update_package(attributes, config)
        update_events(attributes, config)

        if attributes.get('outcome', {}).get('Value') == 'SUCCESS':
            send_next_service_message(
                attributes['service']['Value'],
                attributes['package_id']['Value'],
                config)
    else:
        logging.info('Duplicate event found')
