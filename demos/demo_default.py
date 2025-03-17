import json
import logging
import os
from typing import Dict, List, Any

from annotation.motivation import Motivation

import requests
from kafka import KafkaConsumer, KafkaProducer

from annotation import data_model as ods

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

"""
This is a template for a simple MAS that calls an API and sends a list of annotations back to DiSSCo. 
"""


def start_kafka() -> None:
    """
    Start a kafka listener and process the messages by unpacking the image.
    When done it will republish the object, so it can be validated and stored by the processing service
    """
    consumer = KafkaConsumer(
        os.environ.get("KAFKA_CONSUMER_TOPIC"),
        group_id=os.environ.get("KAFKA_CONSUMER_GROUP"),
        bootstrap_servers=[os.environ.get("KAFKA_CONSUMER_HOST")],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=True,
    )
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_PRODUCER_HOST")],
        value_serializer=lambda m: json.dumps(m).encode("utf-8"),
    )
    for msg in consumer:
        logging.info(f"Received message: {str(msg.value)}")
        json_value = msg.value
        # Indicates to DiSSCo the message has been received by the mas and the job is running.
        # DiSSCo then informs the user of this development
        ods.mark_job_as_running(job_id=json_value.get("jobId"))
        digital_object = json_value.get("object")
        try:
            annotations = map_result_to_annotation(digital_object)
            event = ods.map_to_annotation_event(annotations, json_value["jobId"])
            logging.info(f"Publishing annotation event: {json.dumps(event)}")
            publish_annotation_event(event, producer)
        except Exception as e:
            logging.error(f"Failed to publish annotation event: {e}")
            ods.send_failed_message(json_value.get("jobId"), str(e), producer)


def build_query_string(digital_object: Dict[str, Any]) -> str:
    """
    Builds the query based on the digital object. Fill in your API call here
    :param digital_object: Target of the annotation
    :return: query string to some example API
    """
    return f"https://example.api.com/search?value={digital_object.get('some parameter of interest')}"  # Use your API here


def publish_annotation_event(
    annotation_event: Dict[str, Any], producer: KafkaProducer
) -> None:
    """
    Send the annotation to the Kafka topic
    :param annotation_event: The formatted list of annotations
    :param producer: The initiated Kafka producer
    :return: Will not return anything
    """
    logging.info(f"Publishing annotation: {str(annotation_event)}")
    producer.send(os.environ.get("KAFKA_PRODUCER_TOPIC"), annotation_event)


def map_result_to_annotation(digital_object: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Given a target object, computes a result and maps the result to an openDS annotation
    :param digital_object: the target object of the annotation
    :return: List of annotations
    """
    # Your query here
    query_string = build_query_string(digital_object)
    timestamp = ods.timestamp_now()
    # Run API call and compute value(s) of the annotation
    oa_values = run_api_call(timestamp, query_string)

    if not oa_values:
        # If the API call does not return a result, that information should still be captured in an annotation
        return built_no_match_annotation(
            timestamp,
            digital_object[ods.DCTERMS_ID],
            digital_object[ods.ODS_TYPE],
            query_string,
        )

    annotations = list()
    """
    It is up to the developer to determine the most appropriate selector. A Class 
    Selector is used if the annotation targets a whole class (or entire specimen), 
    a Field Selector is used if the annotation targets a specific field, and a Fragment
    Selector is used if the annotation targets a Region of Interest (media only). 
    Templates for all 3 selectors are provided in the data_model.py library
    
    For Editing, Commenting, and Deleting motivations, the JSON Path of the selector must exist in the target
    (e.g. You can not target the 10th Assertion if there are only 5 assertions on the target)
    For Adding motivations, you may target a path that doesn't exist, as you are adding information to the specimen
    (e.g. adding the first Assertion to a target) 
    """

    selector_assertion = ods.build_class_selector("$['ods:hasAssertions'][0]")
    selector_entity_relationship = ods.build_class_selector(
        "$['ods:hasEntityRelationship'][0]"
    )
    selector_field = ods.build_term_selector("$['ods:topicDomain']")

    # Make an annotation for each oa:value produced
    annotations.append(
        ods.map_to_annotation(
            timestamp,
            Motivation.ADDING,
            oa_values[0],
            selector_assertion,
            digital_object[ods.DCTERMS_ID],
            digital_object[ods.ODS_TYPE],
            query_string,
        )
    )
    annotations.append(
        ods.map_to_annotation(
            timestamp,
            Motivation.ADDING,
            oa_values[1],
            selector_entity_relationship,
            digital_object[ods.DCTERMS_ID],
            digital_object[ods.ODS_TYPE],
            query_string,
        )
    )
    annotations.append(
        ods.map_to_annotation(
            timestamp,
            Motivation.COMMENTING,
            oa_values[2],
            selector_field,
            digital_object[ods.DCTERMS_ID],
            digital_object[ods.ODS_TYPE],
            query_string,
        )
    )

    return annotations


def built_no_match_annotation(
    timestamp: str, target_id: str, target_type: str, query_str: str
) -> List[Dict[str, Any]]:
    """
    If no values are produced, it is best practice to inform the user that the MAS did not produce an annotation
    The motivation for a "no-annotation annotation" should be oa:commenting
    The selector MUST specify a field that exists and is nonnull in the target
    """
    return [
        ods.map_to_annotation(
            timestamp,
            Motivation.COMMENTING,
            "Unable to determine value for annotation",
            ods.build_term_selector("$['ods:hasAssertions']"),
            target_id,
            target_type,
            query_str,
        )
    ]


def run_api_call(timestamp: str, query_string: str) -> List[str]:
    """
    Run API call or performs some computation on the target object
    :param timestamp: The timestamp of the annotation
    :query_string: Query string for the API call
    :return: Value of the annotation (maps to oa:value)
    :raises: request exception on failed API call
    """
    try:

        response = requests.get(query_string)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"API call failed: {e}")
        raise requests.RequestException

    response_json = json.loads(response.content)
    """
    It is up to the MAS developer to determine the best format for the value of the annotation.
    It may be appropriate to send an annotation as an Entity Relationship, if it describes
    a relationship to an external resource, or an Assertion, if the mas has performed 
    some computation or measurement. It may also map to the Georeference class, examples
    of which can be found on our demo GitHub page: https://github.com/DiSSCo/demo-enrichment-service-image/tree/main/osm-georeferencing
    """
    # Return this if response is a computation or measurement
    assertion = json.dumps(
        ods.map_to_assertion(
            timestamp, "pixelWidthX", response_json["pixelWidthX"], "pixels", "https://"
        )
    )
    # Return this if response is an entity relationship
    entity_relationship = json.dumps(
        ods.map_to_entity_relationship(
            "hasRelatedResourceIdentifier", response_json["identifier"], timestamp
        )
    )
    # Return something else - such as the raw response - if the response can not be structured in another way (not recommended)

    """
    It is possible your MAS produces multiple annotations - this is supported. 
    One annotation can be created for each oa:value computed in this step
    """
    return [assertion, entity_relationship, json.dumps(response_json)]
