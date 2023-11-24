import importlib
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, List

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

from mage_ai.shared.config import BaseConfig
from mage_ai.streaming.constants import DEFAULT_BATCH_SIZE, DEFAULT_TIMEOUT_MS
from mage_ai.streaming.sources.base import BaseSource
from mage_ai.streaming.sources.shared import SerDeConfig, SerializationMethod


class SecurityProtocol(str, Enum):
    SASL_PLAINTEXT = 'SASL_PLAINTEXT'
    SASL_SSL = 'SASL_SSL'
    SSL = 'SSL'


@dataclass
class SASLConfig:
    mechanism: str = 'PLAIN'
    username: str = None
    password: str = None


@dataclass
class SSLConfig:
    cafile: str = None
    certfile: str = None
    keyfile: str = None
    password: str = None
    check_hostname: bool = False


@dataclass
class KafkaConfig(BaseConfig):
    bootstrap_server: str
    consumer_group: str
    api_version: str = '0.10.2'
    batch_size: int = DEFAULT_BATCH_SIZE
    timeout_ms: int = DEFAULT_TIMEOUT_MS
    include_metadata: bool = False
    security_protocol: SecurityProtocol = None
    ssl_config: SSLConfig = None
    sasl_config: SASLConfig = None
    serde_config: SerDeConfig = None
    topic: str = None
    topics: List = field(default_factory=list)

    @classmethod
    def parse_config(cls, config: Dict) -> Dict:
        ssl_config = config.get('ssl_config')
        sasl_config = config.get('sasl_config')
        serde_config = config.get('serde_config')
        if ssl_config and isinstance(ssl_config, dict):
            config['ssl_config'] = SSLConfig(**ssl_config)
        if sasl_config and isinstance(sasl_config, dict):
            config['sasl_config'] = SASLConfig(**sasl_config)
        if serde_config and isinstance(serde_config, dict):
            config['serde_config'] = SerDeConfig(**serde_config)
        return config


class KafkaSource(BaseSource):
    config_class = KafkaConfig

    def init_client(self):
        if not self.config.topic and not self.config.topics:
            raise Exception('Please specify topic or topics in the Kafka config.')

        self._print('Start initializing consumer.')
        consumer_config = {
            'bootstrap.servers': self.config.bootstrap_server,
            'group.id': self.config.consumer_group,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'api.version.request': True,
            # Add additional configuration as needed
        }

        # Add security protocols to the configuration
        if self.config.security_protocol in [SecurityProtocol.SSL, SecurityProtocol.SASL_SSL]:
            consumer_config['security.protocol'] = self.config.security_protocol.value
            if self.config.ssl_config.cafile:
                consumer_config['ssl.ca.location'] = self.config.ssl_config.cafile
            if self.config.ssl_config.certfile:
                consumer_config['ssl.certificate.location'] = self.config.ssl_config.certfile
            if self.config.ssl_config.keyfile:
                consumer_config['ssl.key.location'] = self.config.ssl_config.keyfile
            if self.config.ssl_config.password:
                consumer_config['ssl.key.password'] = self.config.ssl_config.password
            consumer_config[
                'ssl.endpoint.identification.algorithm'] = '' if not self.config.ssl_config.check_hostname else 'https'

        if self.config.security_protocol in [SecurityProtocol.SASL_PLAINTEXT, SecurityProtocol.SASL_SSL]:
            consumer_config['sasl.mechanism'] = self.config.sasl_config.mechanism
            consumer_config['sasl.username'] = self.config.sasl_config.username
            consumer_config['sasl.password'] = self.config.sasl_config.password

        # Initialize the schema class if needed
        self.schema_class = None
        if self.config.serde_config and self.config.serde_config.serialization_method == SerializationMethod.PROTOBUF:
            schema_classpath = self.config.serde_config.schema_classpath
            if schema_classpath is not None:
                self._print(f'Loading message schema from {schema_classpath}')
                parts = schema_classpath.split('.')
                if len(parts) >= 2:
                    class_name = parts[-1]
                    libpath = '.'.join(parts[:-1])
                    self.schema_class = getattr(
                        importlib.import_module(libpath),
                        class_name,
                    )

        # Setting up the Avro consumer if needed
        if self.config.serde_config and self.config.serde_config.serialization_method == SerializationMethod.AVRO:
            schema_registry_client = SchemaRegistryClient({'url': self.config.serde_config.schema_registry_url})
            string_deserializer = StringDeserializer('utf_8')
            avro_deserializer = AvroDeserializer(schema_registry_client=schema_registry_client)

            consumer_config.update({
                'key.deserializer': string_deserializer,
                'value.deserializer': avro_deserializer,
            })
            self.consumer = AvroConsumer(consumer_config)
        else:
            self.consumer = Consumer(consumer_config)

        # Subscribe to the topic
        if self.config.topic:
            topics = [self.config.topic]
        else:
            topics = self.config.topics
        self.consumer.subscribe(topics)
        self._print('Consumer initialized.')