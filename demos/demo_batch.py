import json
import logging
import os
from typing import Dict, List, Tuple, Any

import requests
from kafka import KafkaConsumer, KafkaProducer

from annotation import data_model as ods
from annotation.motivation import Motivation

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

"""
This is a template for a simple MAS that calls an API and sends an annotation back to DiSSCo.
The MAS also includes parameters for batching, allowing DiSSCo to apply the same annotation
to multiple specimen that meet the same criteria as the original target
"""


def start_kafka() -> None:
    """
    Start a kafka listener and process the messages by unpacking the Digital Object.
    When done it will publish the resulting annotation to the DiSSCo Annotation Processing Service
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
        try:
            logging.info(f"Received message: {str(msg.value)}")
            json_value = msg.value
            # Indicates to DiSSCo the message has been received by the mas and the job is running.
            # DiSSCo then informs the user of this development
            ods.mark_job_as_running(job_id=json_value.get("jobId"))
            digital_object = json_value.get("object")
            batching_requested = json_value.get("batchingRequested")
            annotations, batch_metadata = map_result_to_annotation(
                digital_object, batching_requested
            )
            event = ods.map_to_annotation_event_batch(
                annotations, json_value["jobId"], batch_metadata
            )
            logging.info(f"Publishing annotation event: {json.dumps(event)}")
            publish_annotation_event(event, producer)
        except Exception as e:
            logging.error(f"Failed to publish annotation event: {e}")


def build_query_string(digital_object: Dict[str, Any]) -> str:
    """
    Builds the query based on the digital object. Fill in your API call here
    :param digital_object: Target of the annotation, used to pull some search parameters from
    :return: query string to some example API
    """
    return f"https://example.api.com/search?value={digital_object.get('some parameter of interest')}"


def publish_annotation_event(
    annotation_event: Dict[str, Any], producer: KafkaProducer
) -> None:
    """
    Send the annotation to the Kafka topic
    :param annotation_event: The formatted list of annotations
    :param producer: The initiated Kafka producer
    :return: Will not return anything
    """
    logging.info("Publishing annotation: " + str(annotation_event))
    producer.send(os.environ.get("KAFKA_PRODUCER_TOPIC"), annotation_event)


def map_result_to_annotation(
    digital_object: Dict[str, Any], batching_requested: bool
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Given a target object, computes a result and maps the result to an openDS annotation
    :param digital_object: the target object of the annotation
    :param batching_requested: Indicates if the scheduling client requested this operation be batched
    :return: List of annotations
    """
    timestamp = ods.timestamp_now()
    annotations = list()
    batch_metadata = list()
    place_in_batch = 0
    # In this example, we create a separate annotation for each item in the ods:hasEvent array
    for event in digital_object.get("ods:hasEvents"):
        query_string = build_query_string(event)
        oa_value = run_api_call(query_string)
        selector_assertion = ods.build_class_selector(
            "JsonPath to class of interest"  # Insert class of interest here
        )
        annotation = ods.map_to_annotation(
            timestamp,
            Motivation.ADDING,
            oa_value,
            selector_assertion,
            digital_object[ods.DCTERMS_ID],
            digital_object[ods.ODS_TYPE],
            query_string,
        )
        if batching_requested:
            # ods:placeInBatch must correspond to an equal value in the batch metadata
            annotation["ods:placeInBatch"] = place_in_batch
            batch_metadata.append(build_batch_metadata(digital_object, place_in_batch))
            place_in_batch = place_in_batch + 1

    # If batching is not requested, it will be empty and will not be added to the event
    return annotations, batch_metadata


def build_batch_metadata(
    digital_object: Dict[str, Any], place_in_batch: int
) -> Dict[str, Any]:
    """
    Given the parameters used to calculate the result, create an object that
    captures these parameters. That way, DiSSCo can apply the same annotation
    to all objects that would result in this annotation, if it were run on them.
    :param digital_object: target of the annotation
    :param place_in_batch: integer value that indicates which annotation in the
    annotation event this batch metadata applies. There must be a corresponding value
    in the annotation
    :return: batch metadata object. The search parameters will be used to create
    a search query to find all relevant objects.
    """
    return {
        "ods:placeInBatch": place_in_batch,
        "searchParams": [
            {
                "inputField": "JsonPath of the field of interest",
                "inputValue": digital_object["field of interest"],
            },
            {
                "inputField": "JsonPath of another field used in this MAS",
                "inputValue": digital_object["second field of interest"],
            },
        ],
    }


def run_api_call(query_string: str) -> str:
    """
    Run API call or performs some computation on the target object
    :param query_string: Location of request
    :return: Value of the annotation (maps to oa:value)
    :raises Request exception
    """
    try:
        response = requests.get(query_string)
        response.raise_for_status()
        response_json = json.loads(response.content)
    except requests.RequestException as e:
        logging.error(f"API call failed: {e}")
        raise requests.RequestException
    """
    It is up to the MAS developer to determine the best format for the value of the annotation.
    """
    oa_value = response_json["resultValue"]
    return json.dumps(oa_value)
