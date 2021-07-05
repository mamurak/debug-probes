from confluent_kafka import Producer, Consumer
import requests


kafka_bridge_request_headers = {'content-type': 'application/vnd.kafka.json.v2+json'}


def kafka_producer(bootstrap_service_url):
    return Producer({'bootstrap.servers': bootstrap_service_url})


def kafka_consumer(bootstrap_service_url, group_id='my-group',
                   topic='my-topic'):
    consumer = Consumer(
        {
            'bootstrap.servers': bootstrap_service_url,
            'group.id': group_id,
        }
    )
    consumer._subscribe([topic])
    return consumer


def publish_to_bridge(bridge_service_url, *messages, topic='my-topic'):
    payload = _to_bridge_payload(*messages)
    topic_url = _to_topic_url(bridge_service_url, topic)
    return requests.post(topic_url, json=payload, headers=kafka_bridge_request_headers)


def _to_bridge_payload(*items):
    payload = {
        'records': [{'value': item} for item in items]
    }
    return payload


def _to_topic_url(bridge_service_url, topic):
    return f'{bridge_service_url}/topics/{topic}'


class KafkaHttpClient:
    def __init__(self, bridge_service_name, port=8080, topic='my-topic', group_id='my-group',
                 client_id='my-client'):
        self.bridge_url = f'http://{bridge_service_name}:{port}'
        self.topic = topic
        self.group_id = group_id
        self.client_id = client_id
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
            'enable.auto.commit': False,
        }
        response = requests.post(
            registration_url, json=client_config, headers=kafka_bridge_request_headers
        )
        return response.json()['base_uri']

    def _subscribe(self):
        requests.post(
            self.client_url+'/subscription',
            json={'topics': [self.topic]},
            headers=kafka_bridge_request_headers
        )

    def poll(self):
        headers = {'accept': 'application/vnd.kafka.json.v2+json'}
        response = requests.get(self.client_url+'/records', headers=headers)
        return [item['value'] for item in response]
