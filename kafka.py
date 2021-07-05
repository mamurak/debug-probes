from confluent_kafka import Producer, Consumer
import requests


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
    consumer.subscribe(topic)
    return consumer


def publish_to_bridge(bridge_service_url, *messages, topic='my-topic'):
    headers = {'content-type': 'application/vnd.kafka.json.v2+json'}
    payload = _to_bridge_payload(*messages)
    topic_url = _to_topic_url(bridge_service_url, topic)
    return requests.post(topic_url, payload, headers=headers)


def _to_bridge_payload(*items):
    payload = {
        'records': [{'value': item} for item in items]
    }
    return payload


def _to_topic_url(bridge_service_url, topic):
    return f'{bridge_service_url}/topics/{topic}'
