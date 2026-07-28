## Publish Raphtory anomaly scores to Kafka using the VICOM message template.

import json
import time
import uuid

import kafka


def make_producer(bootstrap, username, password,
                  security_protocol="SASL_PLAINTEXT", sasl_mechanism="PLAIN"):
    """Build a KafkaProducer from explicit settings (no environment lookups)."""
    return kafka.KafkaProducer(
        bootstrap_servers=bootstrap,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=username,
        sasl_plain_password=password,
        # default=str keeps stray numpy/pandas types serialisable
        value_serializer=lambda m: json.dumps(m, default=str).encode("utf-8"),
    )


def build_message(findings, kg_version="v1.0", request_id=None):
    """build the result message.

    findings: list of dicts with keys
        typeEntity, entityID, risk_level  (+ optional name, description, additional_info)
    """

    now_ms = int(time.time() * 1000)

    indicators = [{
        "id": str(uuid.uuid4()),
        "typeEntity": f["typeEntity"],
        "entityID": f["entityID"],
        "risk_level": f["risk_level"],
        "name": f.get("name", "Raphtory based risk score"),
        "description": f.get("description", "Raphtory based risk score"),
        "additional_info": f.get("additional_info"),
    } for f in findings]

    def ids_of(type_name):
        t = type_name.lower()
        return [f["entityID"] for f in findings if f["typeEntity"].lower() == t]

    kg = {
        "versionId": kg_version,
        "tenders": [{"tenderId": e} for e in ids_of("Tender")],
        "organizations": [{"organizationId": e} for e in ids_of("Organization")],
        "publicEntities": [{"publicEntityId": e} for e in ids_of("PublicEntity")],
    }

    return {
        "requestID": request_id or str(uuid.uuid4()),
        "org": {"name": " Universidad Politécnica de Madrid"},
        "analysis": {
            "toolId": "Raphtory_Analysis",
            "toolName": "Raphtory",
            "startTime": now_ms,
            "endTime": now_ms,
            "indicators": indicators,
        },
        "source": {"kg": kg},
    }




def publish_scores(producer, topic, findings, **kwargs):
    """Build the template message from findings and publish it to `topic`."""
    msg = build_message(findings, **kwargs)
    producer.send(topic, value=msg)
    producer.flush()
    return msg