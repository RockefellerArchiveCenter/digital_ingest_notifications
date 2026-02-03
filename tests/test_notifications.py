#!/usr/bin/env python3

import json
from pathlib import Path
from unittest.mock import call, patch

import boto3
import pytest
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID
from requests.exceptions import HTTPError

from src.handle_digital_ingest_notifications import (
    get_config, lambda_handler, send_http_request, send_next_services_message,
    update_events, update_package)

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
@patch('src.handle_digital_ingest_notifications.update_package')
@patch('src.handle_digital_ingest_notifications.update_events')
@patch('src.handle_digital_ingest_notifications.send_next_services_message')
@pytest.mark.parametrize('data_from_file',
                         ['success_message.json'], indirect=True)
def test_success_notification(
        mock_start, mock_events, mock_package, mock_config, data_from_file):
    attributes = data_from_file['Records'][0]['messageAttributes']
    package_id = '20f8da26e268418ead4aa2365f816a08'
    service = 'validation'
    outcome = 'SUCCESS'
    lambda_handler(data_from_file, None)
    mock_config.assert_called_once()
    mock_start.assert_called_once_with(
        'validation',
        attributes['package_id']['stringValue'],
        'digitization',
        None,
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
        {'identifier': '20f8da26e268418ead4aa2365f816a08', 'origin': 'digitization'})


@patch('src.handle_digital_ingest_notifications.get_config')
@patch('src.handle_digital_ingest_notifications.update_package')
@patch('src.handle_digital_ingest_notifications.update_events')
@patch('src.handle_digital_ingest_notifications.send_next_services_message')
@pytest.mark.parametrize('data_from_file',
                         ['success_message_with_size.json'], indirect=True)
def test_success_notification_with_size(
        mock_start, mock_events, mock_package, mock_config, data_from_file):
    attributes = data_from_file['Records'][0]['messageAttributes']
    package_id = '20f8da26e268418ead4aa2365f816a08'
    service = 'validation'
    outcome = 'SUCCESS'
    lambda_handler(data_from_file, None)
    mock_config.assert_called_once()
    mock_start.assert_called_once_with(
        'validation',
        attributes['package_id']['stringValue'],
        'digitization',
        attributes['size']['stringValue'],
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
        {'identifier': '20f8da26e268418ead4aa2365f816a08', 'origin': 'digitization'})


@patch('src.handle_digital_ingest_notifications.get_config')
@patch('src.handle_digital_ingest_notifications.update_package')
@patch('src.handle_digital_ingest_notifications.update_events')
@patch('src.handle_digital_ingest_notifications.send_next_services_message')
@pytest.mark.parametrize('data_from_file',
                         ['failure_message.json'], indirect=True)
def test_failure_notification(
        mock_start, mock_events, mock_package, mock_config, data_from_file):
    """Assert failure notifications are handled correctly"""
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


@patch('src.handle_digital_ingest_notifications.get_config')
@patch('src.handle_digital_ingest_notifications.update_package')
@patch('src.handle_digital_ingest_notifications.update_events')
@patch('src.handle_digital_ingest_notifications.send_next_services_message')
@pytest.mark.parametrize('data_from_file',
                         ['success_message_missing_attributes.json'], indirect=True)
def test_missing_attributes(
        mock_start, mock_events, mock_package, mock_config, data_from_file):
    """Assert handling when required attributes are missing."""

    lambda_handler(data_from_file, None)

    mock_config.assert_called_once()
    for m in [mock_start, mock_events, mock_package]:
        m.assert_not_called()


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


@mock_aws
def test_start_next_service():
    package_id = '123456789'
    package_size = '987654'
    sns_topic_name = 'digital_ingest_topic.fifo'
    sns = boto3.client('sns', region_name='us-east-1')
    topic_arn = sns.create_topic(
        Name=sns_topic_name,
        Attributes={
            "FifoTopic": 'true',
            "ContentBasedDeduplication": 'true',
        }
    )['TopicArn']
    config = {'SNS_TOPIC': topic_arn}
    sqs_conn = boto3.resource("sqs", region_name="us-east-1")
    queue_name = "test-queue.fifo"
    sqs_conn.create_queue(
        QueueName=queue_name,
        Attributes={
            "FifoQueue": 'true',
            "ContentBasedDeduplication": 'true',
        })
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=f"arn:aws:sqs:us-east-1:{DEFAULT_ACCOUNT_ID}:{queue_name}",
    )

    send_next_services_message(
        'foo',
        package_id,
        'digitization',
        package_size,
        config)  # no next service defined

    queue = sqs_conn.get_queue_by_name(QueueName=queue_name)
    messages = queue.receive_messages(MaxNumberOfMessages=1)
    assert len(messages) == 0

    send_next_services_message(
        'digital_ingest_discovery',
        package_id,
        'digitization',
        package_size,
        config)

    queue = sqs_conn.get_queue_by_name(QueueName=queue_name)
    messages = queue.receive_messages(MaxNumberOfMessages=2)
    assert len(messages) == 2
    assembly_message_body = json.loads(messages[0].body)
    assert assembly_message_body['MessageAttributes']['package_id']['Value'] == package_id
    assert assembly_message_body['MessageAttributes']['requested_status']['Value'] == 'START'
    assert assembly_message_body['MessageAttributes']['service']['Value'] == 'digital_ingest_assembly'
    iiif_message_body = json.loads(messages[1].body)
    assert iiif_message_body['MessageAttributes']['package_id']['Value'] == package_id
    assert iiif_message_body['MessageAttributes']['requested_status']['Value'] == 'START'
    assert iiif_message_body['MessageAttributes']['service']['Value'] == 'iiif_derivatives'


@mock_aws
def test_iiif_service_not_started():
    """Asserts only assembly is started for non-digitization packages."""
    package_id = '123456789'
    package_size = '987654'
    sns_topic_name = 'digital_ingest_topic.fifo'
    sns = boto3.client('sns', region_name='us-east-1')
    topic_arn = sns.create_topic(
        Name=sns_topic_name,
        Attributes={
            "FifoTopic": 'true',
            "ContentBasedDeduplication": 'true',
        }
    )['TopicArn']
    config = {'SNS_TOPIC': topic_arn}
    sqs_conn = boto3.resource("sqs", region_name="us-east-1")
    queue_name = "test-queue.fifo"
    sqs_conn.create_queue(
        QueueName=queue_name,
        Attributes={
            "FifoQueue": 'true',
            "ContentBasedDeduplication": 'true',
        })
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=f"arn:aws:sqs:us-east-1:{DEFAULT_ACCOUNT_ID}:{queue_name}",
    )

    send_next_services_message(
        'digital_ingest_discovery',
        package_id,
        'av_digitization',
        package_size,
        config)

    queue = sqs_conn.get_queue_by_name(QueueName=queue_name)
    messages = queue.receive_messages(MaxNumberOfMessages=2)
    assert len(messages) == 1
    assembly_message_body = json.loads(messages[0].body)
    assert assembly_message_body['MessageAttributes']['package_id']['Value'] == package_id
    assert assembly_message_body['MessageAttributes']['requested_status']['Value'] == 'START'
    assert assembly_message_body['MessageAttributes']['service']['Value'] == 'digital_ingest_assembly'


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


@patch('requests.Session.get')
def test_send_http_request(mock_get):
    """Tests HTTP requests result in expected behavior"""

    class MockResponse(object):
        """Class used to mock HTTP responses"""

        def __init__(self, json_data, status_code, **kwargs):
            """Sets data, status code, and any other data passed in."""
            self.json_data = json_data
            self.status_code = status_code
            for k in kwargs:
                setattr(self, k, kwargs[k])

        def json(self):
            """Mocks the json method of an HTTP response"""
            return self.json_data

        def raise_for_status(self):
            if self.status_code != 200:
                self.text = "This is an error"
                raise HTTPError(response=self)
            pass

    mock_get.return_value = MockResponse({}, 200)
    output = send_http_request("example.com", 'get')
    assert output == {}

    output = send_http_request("example.com", 'get', data={"foo": "bar"})
    assert output == {}

    mock_get.return_value = MockResponse({}, 400)
    with pytest.raises(HTTPError):
        send_http_request("example.com", 'get')
