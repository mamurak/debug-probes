from logging import getLogger
from pprint import pformat

from confluent_kafka import Producer, Consumer
import requests


log = getLogger()


def kafka_producer(bootstrap_service_url):
    log.info(f'Instantiating Kafka producer consuming {bootstrap_service_url}.')
    return Producer({'bootstrap.servers': bootstrap_service_url})


def kafka_consumer(bootstrap_service_url, group_id='my-group',
                   topic='my-topic'):
    log.info(f'Instantiating Kafka consumer of group "{group_id}" consuming topic "{topic}"'
             f' from {bootstrap_service_url}.')
    consumer = Consumer(
        {
            'bootstrap.servers': bootstrap_service_url,
            'group.id': group_id,
        }
    )
    consumer.subscribe([topic])
    return consumer


def publish_to_bridge(bridge_service_name, *messages, port=8080, topic='my-topic'):
    payload = _to_bridge_payload(*messages)
    topic_url = _to_topic_url(bridge_service_name, port, topic)
    headers = {'content-type': 'application/vnd.kafka.json.v2+json'}
    
    log.info(f'Publishing message to topic "{topic}" at {topic_url}.')
    log.debug(f'Payload: {pformat(payload)}')
    
    response = requests.post(topic_url, json=payload, headers=headers)
    log.info(f'Received response {response}.')
    log.debug(f'Response content: {response.json()}')
    
    return response


def _to_bridge_payload(*items):
    payload = {
        'records': [{'value': item} for item in items]
    }
    return payload


def _to_topic_url(bridge_service_name, port, topic):
    return f'http://{bridge_service_name}:{port}/topics/{topic}'


class KafkaHttpClient:
    def __init__(self, bridge_service_name, port=8080, topic='my-topic', group_id='my-group',
                 client_id='my-client'):
        self.bridge_url = f'http://{bridge_service_name}:{port}'
        self.topic = topic
        self.group_id = group_id
        self.client_id = client_id
        log.info(f'Instantiating {self}.')
        self.client_url = self._register_client()
        self._subscribe()

    def __str__(self):
        instance_string = (
            f'KafkaHttpClient<bridge: {self.bridge_url}, topic: {self.topic},'
            f' group ID: {self.group_id}, client ID: {self.client_id}>'
        )
        return instance_string

    def _register_client(self):
        registration_url = f'{self.bridge_url}/consumers/{self.group_id}'
        client_config = {
            'name': self.client_id,
            'format': 'json',
            'auto.offset.reset': 'earliest',
            'fetch.min.bytes': 512,
            'enable.auto.commit': True,
        }
        log.info(f'{self} registering at {registration_url}.')
        response = requests.post(
            registration_url,
            json=client_config,
            headers={'content-type': 'application/vnd.kafka.v2+json'}
        )
        log.debug(f'Received response {response}.')
        response_json_body = response.json()
        log.debug(f'Received JSON content {pformat(response_json_body)}.')
        return response_json_body['base_uri']

    def _subscribe(self):
        log.info(f'{self} subscribing to topic {self.topic}.')
        response = requests.post(
            self.client_url+'/subscription',
            json={'topics': [self.topic]},
            headers={'content-type': 'application/vnd.kafka.v2+json'},
        )
        log.debug(f'Received response {response}.')

    def poll(self):
        log.info(f'{self} polling new messages.')
        headers = {'accept': 'application/vnd.kafka.json.v2+json'}
        response = requests.get(self.client_url+'/records', headers=headers)
        log.debug(f'Received response {response}.')
        response_body = response.json()
        log.debug(f'Received response body {pformat(response_body)}.')
        return [item['value'] for item in response_body]
