"""
Kafka connection of the risk-assessment tool: settings and topics from
.env, the SASL_SSL connection, producer, consumers and publishing.
"""

import json
import os
import re

import kafka
from dotenv import find_dotenv
from kafka.serializer import Serializer


# Settings and topics (.env)
_TOPIC_SUFFIX = {"request": "analyse", "results": "results_available"}

def _split(value):
    """'it,sl' / 'it sl' -> ['it', 'sl']"""
    return [v for v in re.split(r"[,\s]+", value or "") if v]


def pilot_topic(pilot, kind):
    """Request ('request') or results ('results') topic of one pilot.

    In order of precedence:
      1. KAFKA_<KIND>_TOPIC_<PILOT>               KAFKA_REQUEST_TOPIC_IT=it_analyse
      2. the entry of KAFKA_<KIND>_TOPICS starting with "<pilot>_"
             KAFKA_REQUEST_TOPICS=it_analyse,sl_analyse
             KAFKA_RESULTS_TOPICS=it_results_available,sl_results_available
      3. the contract's naming: <pilot>_analyse / <pilot>_results_available
    """
    env = f"KAFKA_{kind.upper()}_TOPIC"
    override = os.getenv(f"{env}_{pilot.upper()}")
    if override:
        return override
    for topic in _split(os.getenv(f"{env}S")):
        if topic.lower().startswith(f"{pilot.lower()}_"):
            return topic
    return f"{pilot}_{_TOPIC_SUFFIX[kind]}"


def _path(path):
    """A relative credential path is relative to the .env file (found from the
    working directory upwards, as load_dotenv does) """
    if not path or os.path.isabs(path):
        return path
    env_file = find_dotenv(usecwd=True)
    base = os.path.dirname(env_file) if env_file else os.getcwd()
    return os.path.normpath(os.path.join(base, path))


def _env(*names, default=None):
    """First non-empty variable among `names` (production name first, then
    the older names this tool used before)."""
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def kafka_settings():
    """Broker connection settings from the environment.

    Same variables as the CEDAR template (secure-kafka/example-code/.env.example):
        KAFKA_BOOTSTRAP_SERVERS                 host:port[,host:port]
        KAFKA_SASL_USERNAME / KAFKA_SASL_PASSWORD   lines 2 and 3 of cred.txt
        KAFKA_KEY_PASSWORD                      line 1 of cred.txt (key passphrase)
        KAFKA_CA_FILE / KAFKA_CERT_FILE / KAFKA_KEY_FILE   
    plus:
        KAFKA_CREDENTIALS_DIR   the extracted <user>/ folder; the three files are
                                then found there under their production names
                                (kafka-cedar-ca-1.crt, <user>.certificate.pem,
                                <user>.key) unless KAFKA_*_FILE is set
    
        KAFKA_SECURITY_PROTOCOL default: SASL_SSL with a CA file,
                                SASL_PLAINTEXT with only a username, else PLAINTEXT
        
        KAFKA_SASL_MECHANISM    default PLAIN
    Relative paths are relative to the .env file.
    """
    username = _env("KAFKA_SASL_USERNAME")
    folder = _path(_env("KAFKA_CREDENTIALS_DIR"))
    in_folder = lambda name: os.path.join(folder, name) if folder and name else None
    return {
        "bootstrap": _env("KAFKA_BOOTSTRAP_SERVERS",default="localhost:9092"),
        "username": username,
        "password": _env("KAFKA_SASL_PASSWORD"),
        "security_protocol": _env("KAFKA_SECURITY_PROTOCOL"),
        "sasl_mechanism": _env("KAFKA_SASL_MECHANISM", default="PLAIN"),
        "ca_file": _path(_env("KAFKA_CA_FILE")) or in_folder("kafka-cedar-ca-1.crt"),
        "cert_file": _path(_env("KAFKA_CERT_FILE"))
                     or in_folder(username and f"{username}.certificate.pem"),
        "key_file": _path(_env("KAFKA_KEY_FILE"))
                    or in_folder(username and f"{username}.key"),
        "key_password": _env("KAFKA_KEY_PASSWORD"),
        "check_hostname": _env("KAFKA_SSL_CHECK_HOSTNAME", default="true").lower()
                          not in {"0", "false", "no"},
    }

# Producer / consumers
class KafkaConfigError(ValueError):
    """A setting or credential file needed for the chosen protocol is missing."""

def _connection(bootstrap="localhost:9092", username=None, password=None,
                security_protocol=None, sasl_mechanism="PLAIN", ca_file=None,
                cert_file=None, key_file=None, key_password=None, check_hostname=True):
    
    if security_protocol is None:
        security_protocol = ("SASL_SSL" if ca_file else
                             "SASL_PLAINTEXT" if username else "PLAINTEXT")
    
    security_protocol = security_protocol.upper()
    conn = {"bootstrap_servers": [s.strip() for s in bootstrap.split(",") if s.strip()],
            "security_protocol": security_protocol}

    if security_protocol.startswith("SASL"):
        if not (username and password):
            raise KafkaConfigError(f"{security_protocol} needs KAFKA_SASL_USERNAME and "
                                   f"KAFKA_SASL_PASSWORD (lines 2 and 3 of cred.txt)")
        
        conn.update(sasl_mechanism=sasl_mechanism,
                    sasl_plain_username=username,
                    sasl_plain_password=password)

    if security_protocol.endswith("SSL"):
        # verify the broker against the CA, and
        # present our own certificate (the broker requires one)
        files = {"KAFKA_CA_FILE": ca_file, "KAFKA_CERT_FILE": cert_file,
                 "KAFKA_KEY_FILE": key_file}
        for name, path in files.items():
            if not path:
                raise KafkaConfigError(f"{security_protocol} needs {name} "
                                       f"(or KAFKA_CREDENTIALS_DIR)")
            if not os.path.isfile(path):
                raise KafkaConfigError(f"{name}: file not found: {path}")
        conn.update(ssl_cafile=ca_file, ssl_certfile=cert_file, ssl_keyfile=key_file,
                    ssl_password=key_password, ssl_check_hostname=check_hostname)
    return conn

class JsonSerializer(Serializer):
    """dict -> UTF-8 JSON bytes. default=str keeps stray numpy/pandas types
    serialisable."""
    def serialize(self, topic, *args):
        return json.dumps(args[-1], ensure_ascii=False, default=str).encode("utf-8")

def make_producer(**settings):
    """Producer for requests and responses (gzip-compressed JSON)."""
    return kafka.KafkaProducer(
        **_connection(**settings),
        compression_type="gzip",
        value_serializer=JsonSerializer(),
    )

def make_consumer(topics, group_id, auto_offset_reset="latest",
                  max_poll_interval_ms=30 * 60 * 1000, **settings):
    
    """Consumer of the request topics,.
    Values come back as raw bytes (parse_request decodes them, so a malformed
    message is logged instead of crashing the loop). One record per poll and
    no auto-commit: the caller commits after each request is answered. The
    long poll interval leaves time for a full graph analysis between polls.
    `group_id` = upm-raphtory   must be this tool's own: the request topics are shared.
    """

    return kafka.KafkaConsumer(
        *topics,
        **_connection(**settings),
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset=auto_offset_reset,
        max_poll_records=1,
        max_poll_interval_ms=max_poll_interval_ms,
    )

def tail_consumer(topics, **settings):

    """Consumer already positioned at the END of `topics` -- create it BEFORE
    used by the kafka/send_request.py to test the module in local settigs"""

    consumer = kafka.KafkaConsumer(**_connection(**settings), group_id=None,
                                   enable_auto_commit=False)
    consumer.topics()                                   # fetch the metadata
    parts = []
    for topic in topics:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            consumer.close()
            raise ValueError(f"topic {topic!r} does not exist on the broker")
        parts += [kafka.TopicPartition(topic, p) for p in partitions]
    consumer.assign(parts)
    consumer.seek_to_end(*parts)
    for tp in parts:
        consumer.position(tp)            # resolve the end offset now, not at the first poll
    return consumer

def publish(producer, topic, message, timeout=30):

    """Send one message and wait for the broker's ack (raises if it is rejected,
    e.g. MessageSizeTooLargeError)."""
    
    producer.send(topic, value=message).get(timeout=timeout)
    return message
