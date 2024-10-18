#!/usr/bin/env python3

import json
import pytest
from pathlib import Path
from unittest.mock import patch

import boto3
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID

from src.handle_digital_ingest_notifications import (
    get_config, lambda_handler, matching_events, send_next_service_message, update_events, update_package)


TABLE_NAME = 'test_table'
ZODIAC_BASEURL = 'https://zodiac.rockarch.org/api'

@pytest.fixture
def data_from_file(request):
    path_to_file = Path(
        "tests",
        "fixtures",
        request.param)
    with open(path_to_file, "r") as read_file:
        data = json.load(read_file)
    return data

@pytest.fixture
def config_fixture():
    return {
        'ZODIAC_BASEURL': ZODIAC_BASEURL
    }


@patch('src.handle_digital_ingest_notifications.get_config')
@patch('src.handle_digital_ingest_notifications.matching_events')
@patch('src.handle_digital_ingest_notifications.update_package')
@patch('src.handle_digital_ingest_notifications.update_events')
@patch('src.handle_digital_ingest_notifications.send_next_service_message')
@pytest.mark.parametrize('data_from_file', ['success_message.json'], indirect=True)
def test_success_notification(mock_start, mock_events, mock_package, mock_matching_events, mock_config, data_from_file):
    attributes = data_from_file['Records'][0]['Sns']['MessageAttributes']
    mock_matching_events.return_value = []
    lambda_handler(data_from_file, None)
    mock_config.assert_called_once()
    mock_start.assert_called_once_with('validation', attributes['package_id']['Value'], mock_config())
    mock_events.assert_called_once_with(attributes, mock_config())
    mock_package.assert_called_once_with(attributes, mock_config())

    # reset mocks
    mock_config.reset_mock()
    mock_start.reset_mock()
    mock_events.reset_mock()
    mock_package.reset_mock()

    mock_matching_events.return_value = [{"foo": "bar"}]
    lambda_handler(data_from_file, None)
    mock_config.assert_called_once()
    mock_start.assert_not_called()
    mock_events.assert_not_called()
    mock_package.assert_not_called()


@patch('src.handle_digital_ingest_notifications.get_config')
@patch('src.handle_digital_ingest_notifications.matching_events')
@patch('src.handle_digital_ingest_notifications.update_package')
@patch('src.handle_digital_ingest_notifications.update_events')
@patch('src.handle_digital_ingest_notifications.send_next_service_message')
@pytest.mark.parametrize('data_from_file', ['failure_message.json'], indirect=True)
def test_failure_notification(mock_start, mock_events, mock_package, mock_matching_events, mock_config, data_from_file):
    """Assert failure notifications are handled correctly"""
    attributes = data_from_file['Records'][0]['Sns']['MessageAttributes']
    mock_matching_events.return_value = []
    lambda_handler(data_from_file, None)
    mock_config.assert_called_once()
    mock_start.assert_not_called()
    mock_events.assert_called_once_with(attributes, mock_config())
    mock_package.assert_called_once_with(attributes, mock_config())


@patch('src.handle_digital_ingest_notifications.send_http_request')
@patch('src.handle_digital_ingest_notifications.construct_event_id')
@pytest.mark.parametrize('data_from_file', ['success_attributes.json'], indirect=True)
def test_create_event(mock_id, mock_http, config_fixture, data_from_file):
    """Assert events are created with correct data"""
    event_id = '123456789'
    mock_id.return_value = event_id
    update_events(data_from_file, config_fixture)
    assert mock_http.call_count == 2
    mock_http.assert_called_with(
        f"{ZODIAC_BASEURL}/events",
        'post',
        {
            'outcome': 'SUCCESS', 
            'service': 'validation', 
            'package': '20f8da26e268418ead4aa2365f816a08', 
            'identifier': event_id
        })


@patch('src.handle_digital_ingest_notifications.send_http_request')
@patch('src.handle_digital_ingest_notifications.construct_event_id')
@patch('src.handle_digital_ingest_notifications.matching_events')
@pytest.mark.parametrize('data_from_file', ['success_attributes.json'], indirect=True)
def test_update_event(mock_matching_events, mock_id, mock_http, config_fixture, data_from_file):
    """Assert event data is updated as expected"""
    event_id = '123456789'
    mock_id.return_value = event_id
    mock_matching_events.return_value = [
        {
            'outcome': 'SUCCESS', 
            'service': 'validation', 
            'package': '20f8da26e268418ead4aa2365f816a08', 
            'identifier': event_id
        }
    ]
    update_events(data_from_file, config_fixture)
    assert mock_http.call_count == 1
    mock_http.assert_called_with(
        f"{ZODIAC_BASEURL}/events",
        'post',
        {
            'outcome': 'SUCCESS', 
            'service': 'validation', 
            'package': '20f8da26e268418ead4aa2365f816a08', 
            'identifier': event_id})
    mock_id.assert_not_called()


@patch('src.handle_digital_ingest_notifications.send_http_request')
@pytest.mark.parametrize('data_from_file', ['success_attributes.json'], indirect=True)
def test_create_package(mock_http, config_fixture, data_from_file):
    """Assert packages are created with the correct data"""
    update_package(data_from_file, config_fixture)
    mock_http.assert_called_once_with(
        f'{ZODIAC_BASEURL}/packages', 
        'post', 
        {'package_id': '20f8da26e268418ead4aa2365f816a08'}
    )


@patch('src.handle_digital_ingest_notifications.send_http_request')
@pytest.mark.parametrize('data_from_file', ['success_attributes_with_data.json'], indirect=True)
def test_create_package_with_data(mock_http, config_fixture, data_from_file):
    """Assert packages are created with the correct data"""
    update_package(data_from_file, config_fixture)
    mock_http.assert_called_once_with(
        f'{ZODIAC_BASEURL}/packages', 
        'post',
        {
            'package_id': '20f8da26e268418ead4aa2365f816a08', 
            'package_data': {'foo': 'bar', 'baz': [{'bus': True, 'buz': False}]}
        }
    )


@patch('src.handle_digital_ingest_notifications.send_http_request')
@pytest.mark.parametrize('data_from_file', ['package_events.json'], indirect=True)
def test_matching_events(mock_http, data_from_file):
    """Assert matching events returns expected results"""
    mock_http.return_value = data_from_file
    assert len(matching_events("package_id", "fornax", "baseurl")) == 1 # matching service
    assert len(matching_events("package_id", "foo", "baseurl")) == 0 # no matching service


@mock_aws
@patch('src.handle_digital_ingest_notifications.get_client_with_role')
def test_start_next_service(mock_role):
    package_id = '123456789'
    sns_topic_name = 'digital_ingest_start_service_topic'
    sns = boto3.client('sns', region_name='us-east-1')
    mock_role.return_value = sns
    topic_arn = sns.create_topic(Name=sns_topic_name)['TopicArn']
    config = {'SERVICE_START_SNS_TOPIC': topic_arn}
    sqs_conn = boto3.resource("sqs", region_name="us-east-1")
    sqs_conn.create_queue(QueueName="test-queue")
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=f"arn:aws:sqs:us-east-1:{DEFAULT_ACCOUNT_ID}:test-queue",
    )

    send_next_service_message('foo', package_id, config) # no next service defined
    
    queue = sqs_conn.get_queue_by_name(QueueName="test-queue")
    messages = queue.receive_messages(MaxNumberOfMessages=1)
    assert len(messages) == 0

    send_next_service_message('ursa_major', package_id, config)
    
    queue = sqs_conn.get_queue_by_name(QueueName="test-queue")
    messages = queue.receive_messages(MaxNumberOfMessages=1)
    message_body = json.loads(messages[0].body)
    assert message_body['MessageAttributes']['package_id']['Value'] == package_id
    assert message_body['MessageAttributes']['requested_status']['Value'] == 'START'
    assert message_body['MessageAttributes']['service']['Value'] == 'fornax'


@mock_aws
def test_config():
    ssm = boto3.client('ssm', region_name='us-east-1')
    path = "/dev/digitized_av_trigger"
    for name, value in [("foo", "bar"), ("baz", "buzz")]:
        ssm.put_parameter(
            Name=f"{path}/{name}",
            Value=value,
            Type="SecureString",
        )
    config = get_config(path)
    assert config == {'foo': 'bar', 'baz': 'buzz'}
