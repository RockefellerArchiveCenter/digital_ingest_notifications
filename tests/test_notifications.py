#!/usr/bin/env python3

import json
from pathlib import Path
from unittest.mock import Mock, call, patch

import boto3
import pytest
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID
from requests.exceptions import HTTPError

from src.handle_digital_ingest_notifications import (get_config,
                                                     lambda_handler,
                                                     matching_events,
                                                     send_next_service_message,
                                                     update_events,
                                                     update_package)

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
@pytest.mark.parametrize('data_from_file',
                         ['success_message.json'], indirect=True)
def test_success_notification(
        mock_start, mock_events, mock_package, mock_matching_events, mock_config, data_from_file):
    attributes = data_from_file['Records'][0]['messageAttributes']
    package_id = '20f8da26e268418ead4aa2365f816a08'
    service = 'validation'
    outcome = 'SUCCESS'
    mock_matching_events.return_value = []
    lambda_handler(data_from_file, None)
    mock_config.assert_called_once()
    mock_start.assert_called_once_with(
        'validation',
        attributes['package_id']['stringValue'],
        mock_config())
    mock_events.assert_called_once_with(
        mock_config(),
        package_id,
        service, outcome,
        'Validation successful',
        None)
    mock_package.assert_called_once_with(
        mock_config(),
        package_id,
        {'identifier': '20f8da26e268418ead4aa2365f816a08'})

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
@pytest.mark.parametrize('data_from_file',
                         ['failure_message.json'], indirect=True)
def test_failure_notification(
        mock_start, mock_events, mock_package, mock_matching_events, mock_config, data_from_file):
    """Assert failure notifications are handled correctly"""
    mock_matching_events.return_value = []
    lambda_handler(data_from_file, None)
    mock_config.assert_called_once()
    mock_start.assert_not_called()
    package_id = '20f8da26e268418ead4aa2365f816a08'
    service = 'validation'
    outcome = 'FAILURE'
    message = 'BagIt validation failed.'
    traceback = 'Much longer traceback.'
    mock_events.assert_called_once_with(
        mock_config(),
        package_id,
        service,
        outcome,
        message,
        traceback)
    mock_package.assert_called_once_with(mock_config(), package_id, None)


@patch('src.handle_digital_ingest_notifications.send_http_request')
@patch('src.handle_digital_ingest_notifications.construct_event_id')
def test_create_success_event(mock_id, mock_http, config_fixture):
    """Assert events are created with correct data"""
    event_id = '123456789'
    mock_id.return_value = event_id
    update_events(
        config_fixture,
        '20f8da26e268418ead4aa2365f816a08',
        'validation',
        'SUCCESS',
        None,
        None)
    mock_http.assert_called_once_with(
        f"{ZODIAC_BASEURL}/events/",
        'post',
        {
            'outcome': 'SUCCESS',
            'service': 'validation',
            'package_identifier': '20f8da26e268418ead4aa2365f816a08',
            'identifier': event_id,
            'message': None,
            'traceback': None
        })


@patch('src.handle_digital_ingest_notifications.send_http_request')
@patch('src.handle_digital_ingest_notifications.construct_event_id')
def test_create_failure_event(mock_id, mock_http, config_fixture):
    """Assert events are created with correct data"""
    event_id = '123456789'
    mock_id.return_value = event_id
    update_events(
        config_fixture,
        '20f8da26e268418ead4aa2365f816a08',
        'validation',
        'FAILURE',
        'BagIt validation failed.',
        'Much longer traceback.')
    mock_http.assert_called_once_with(
        f"{ZODIAC_BASEURL}/events/",
        'post',
        {
            'outcome': 'FAILURE',
            'service': 'validation',
            'package_identifier': '20f8da26e268418ead4aa2365f816a08',
            'message': 'BagIt validation failed.',
            'traceback': 'Much longer traceback.',
            'identifier': event_id
        })


@patch('src.handle_digital_ingest_notifications.send_http_request')
def test_create_package(mock_http, config_fixture):
    """Assert packages are created with the correct data"""
    mock_http.side_effect = [HTTPError(), None]
    update_package(config_fixture, '20f8da26e268418ead4aa2365f816a08', None)
    mock_http.assert_has_calls([
        call(f'{ZODIAC_BASEURL}/packages/20f8da26e268418ead4aa2365f816a08/',
             'patch',
             {
                 'identifier': '20f8da26e268418ead4aa2365f816a08'
             }),
        call(
            f'{ZODIAC_BASEURL}/packages/',
            'post',
            {
                'identifier': '20f8da26e268418ead4aa2365f816a08'}
        )
    ])


@patch('src.handle_digital_ingest_notifications.send_http_request')
def test_create_package_with_data(mock_http, config_fixture):
    """Assert packages are created with the correct data"""
    mock_http.side_effect = [HTTPError(), None]
    data = {
        'identifier': '20f8da26e268418ead4aa2365f816a08',
        'foo': 'bar',
        'baz': [{'bus': True, 'buz': False}]
    }
    update_package(
        config_fixture,
        '20f8da26e268418ead4aa2365f816a08',
        data)
    mock_http.assert_has_calls([
        call(f'{ZODIAC_BASEURL}/packages/20f8da26e268418ead4aa2365f816a08/',
             'patch',
             data),
        call(
            f'{ZODIAC_BASEURL}/packages/',
            'post',
            data
        )
    ])


@patch('src.handle_digital_ingest_notifications.send_http_request')
def test_update_package_with_data(mock_http, config_fixture):
    """Assert packages are created with the correct data"""
    data = {
        'identifier': '20f8da26e268418ead4aa2365f816a08',
        'foo': 'bar',
        'baz': [{'bus': True, 'buz': False}]
    }
    update_package(
        config_fixture,
        '20f8da26e268418ead4aa2365f816a08',
        data)
    mock_http.assert_called_once_with(
        f'{ZODIAC_BASEURL}/packages/20f8da26e268418ead4aa2365f816a08/',
        'patch',
        data
    )


@patch('src.handle_digital_ingest_notifications.send_http_request')
@pytest.mark.parametrize('data_from_file',
                         ['package_events.json'], indirect=True)
def test_matching_events(mock_http, data_from_file):
    """Assert matching events returns expected results"""
    mock_http.return_value = data_from_file
    assert len(
        matching_events(
            "package_id",
            "digital_ingest_assembly",
            "baseurl",
            outcome="SUCCESS")) == 1  # matching service and status
    assert len(
        matching_events(
            "package_id",
            "digital_ingest_assembly",
            "baseurl",
            outcome="FAILURE")) == 0  # matching service, mismatched status
    assert len(
        matching_events(
            "package_id",
            "foo",
            "baseurl",
            outcome="SUCCESS")) == 0  # no matching service
    mock_response = Mock()
    mock_response.status_code = 404
    mock_http.side_effect = HTTPError(response=mock_response)
    assert len(
        matching_events(
            "package_id",
            "foo",
            "baseurl",
            outcome="SUCCESS")) == 0  # 404


@mock_aws
def test_start_next_service():
    package_id = '123456789'
    sns_topic_name = 'digital_ingest_topic'
    sns = boto3.client('sns', region_name='us-east-1')
    topic_arn = sns.create_topic(Name=sns_topic_name)['TopicArn']
    config = {'SNS_TOPIC': topic_arn}
    sqs_conn = boto3.resource("sqs", region_name="us-east-1")
    sqs_conn.create_queue(QueueName="test-queue")
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=f"arn:aws:sqs:us-east-1:{DEFAULT_ACCOUNT_ID}:test-queue",
    )

    send_next_service_message(
        'foo',
        package_id,
        config)  # no next service defined

    queue = sqs_conn.get_queue_by_name(QueueName="test-queue")
    messages = queue.receive_messages(MaxNumberOfMessages=1)
    assert len(messages) == 0

    send_next_service_message('digital_ingest_discovery', package_id, config)

    queue = sqs_conn.get_queue_by_name(QueueName="test-queue")
    messages = queue.receive_messages(MaxNumberOfMessages=1)
    message_body = json.loads(messages[0].body)
    assert message_body['MessageAttributes']['package_id']['Value'] == package_id
    assert message_body['MessageAttributes']['requested_status']['Value'] == 'START'
    assert message_body['MessageAttributes']['service']['Value'] == 'digital_ingest_assembly'


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
