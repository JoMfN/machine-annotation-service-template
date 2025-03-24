from datetime import datetime, timezone
from typing import Dict, Any, List
import os
import requests
from kafka import KafkaProducer
from motivation import Motivation

ODS_TYPE = "ods:fdoType"
AT_TYPE = "@type"
DCTERMS_ID = "dcterms:identifier"
DCTERMS_CREATOR = "dcterms:creator"
AT_ID = "@id"
MAS_ID = os.environ.get("MAS_ID")
MAS_NAME = os.environ.get("MAS_NAME")


def timestamp_now() -> str:
    """
    Create a timestamp in the correct format
    :return: The timestamp as a string
    """
    timestamp = str(datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f"))
    timestamp_cleaned = timestamp[:-3]
    timestamp_timezone = timestamp_cleaned + "Z"
    return timestamp_timezone


def mark_job_as_running(job_id: str):
    """
    Calls DiSSCo's RUNNING endpoint to inform the system that the message has
    been received by the MAS. Doing so will update the status of the job to
    "RUNNING" for any observing users.
    :param job_id: the job id from the kafka message
    """
    query_string = f"{os.environ.get('RUNNING_ENDPOINT')}/{MAS_ID}/{job_id}/running"
    requests.get(query_string)


def build_agent() -> Dict[str, Any]:
    """
    Builds agent based on MAS ID and name
    :return: Agent object
    """
    return {
        AT_ID: f"https://hdl.handle.net/{MAS_ID}",
        AT_TYPE: "schema:SoftwareApplication",
        "schema:name": MAS_NAME,
        "ods:hasRoles": [
            {
                AT_TYPE: "schema:Role",
                "schema:roleName": "machine-annotation-service",
            }
        ],
        "ods:hasIdentifiers": [
            {
                AT_ID: f"https://hdl.handle.net/{MAS_ID}",
                AT_TYPE: "ods:Identifier",
                "dcterms:type": "Handle",
                "dcterms:title": "Handle",
                DCTERMS_ID: f"https://hdl.handle.net/{MAS_ID}",
                "ods:isPartOfLabel": False,
                "ods:gupriLevel": "GloballyUniqueStablePersistentResolvableFDOCompliant",
                "ods:identifierStatus": "Preferred",
            }
        ],
    }


def map_to_assertion(
    timestamp: str,
    measurement_type: str,
    measurement_value: str,
    measurement_unit: str,
    assertion_protocol: str,
) -> Dict[str, Any]:
    """
    Maps the result of some computation to an OpenDS Assertion object
    :param timestamp: timestamp of the annotation
    :param measurement_type: The nature of the measurement, fact, characteristic, or assertion.
    :param measurement_value: The value of the measurement, fact, characteristic, or assertion.
    :param measurement_unit: Recommended best practice is to use a controlled vocabulary such as the Ontology of Units of Measure
    :param assertion_protocol: The protocol used to make the assertion
    :return: Formatted assertion object
    """
    return {
        AT_TYPE: "ods:Assertion",
        "dwc:measurementDeterminedDate": timestamp,
        "dwc:measurementType": measurement_type,
        "dwc:measurementValue": measurement_value,
        DCTERMS_CREATOR: [build_agent()],
        "dwc:measurementUnit": measurement_unit,
        "dwc:measurementMethod": assertion_protocol,
    }


def map_to_entity_relationship(
    relationship_of_resource: str, resource_id: str, timestamp: str
) -> Dict[str, Any]:
    """
    Maps the result of some search to an Entity Relationship object
    :param relationship_of_resource: Maps to dwc:relationshipOfResource
    :param resource_id: ID of related resource, maps to dwc:relatedResourceID and ods:relatedResourceURI
    :param timestamp: timestamp of ER creation
    :return: formatted Entity relationship annotation
    """
    return {
        AT_TYPE: "ods:EntityRelationship",
        "dwc:relationshipOfResource": relationship_of_resource,
        "dwc:relatedResourceID": resource_id,
        "ods:relatedResourceURI": resource_id,
        "dwc:relationshipEstablishedDate": timestamp,
        DCTERMS_CREATOR: [build_agent()],
    }


def map_to_annotation_event(
    annotations: List[Dict[str, Any]], job_id: str
) -> Dict[str, Any]:
    """
    Maps to annotation event to be sent to DiSSCo
    :param annotations: List of annotations produced by DiSSCo
    :param job_id: Job ID (Handle) sent to MAS
    :return: annotation event
    """
    return {"annotations": annotations, "jobId": job_id}


def map_to_annotation_event_batch(
    annotations: List[Dict[str, Any]], job_id: str, batch_metadata: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Builds annotation event with batch metadata to send to DiSSCo
    :param annotations: Annotations produced by MAS
    :param job_id: Job ID (Handle) sent to MAS
    :param batch_metadata: List of batch metadata. Each entry corresponds to an annotation. Ignored if empty
    :return: annotation event
    """
    event = {"annotations": annotations, "jobId": job_id}
    if batch_metadata:
        event["batchMetadata"] = batch_metadata
    return event


def map_to_annotation(
    timestamp: str,
    motivation: Motivation,
    oa_value: str,
    oa_selector: Dict[str, Any],
    target_id: str,
    target_type: str,
    dcterms_ref: str,
) -> Dict[str, Any]:
    """
    Map the result of the API call to an annotation
    :param timestamp: A formatted timestamp of the time the annotation was initialized
    :param motivation: Motivation for the annotation
    :param oa_value: Value of the body of the annotation, the result of the computation
    :param oa_selector: selector of this annotation
    :param target_id: ID of target maps to ods:ID
    :param target_type: target Type, maps to ods:type
    :param dcterms_ref: maps tp dcterms:references, http://purl.org/dc/terms/references
    :return: Returns a formatted annotation Record
    """
    annotation = {
        AT_TYPE: "ods:Annotation",
        "oa:motivation": motivation.value,
        DCTERMS_CREATOR: build_agent(),
        "dcterms:created": timestamp,
        "oa:hasTarget": {
            DCTERMS_ID: target_id,
            AT_ID: target_id,
            ODS_TYPE: target_type,
            AT_TYPE: target_type,
            "oa:hasSelector": oa_selector,
        },
        "oa:hasBody": {
            AT_TYPE: "oa:TextualBody",
            "oa:value": oa_value,
            "dcterms:references": dcterms_ref,
        },
    }
    return annotation


def build_class_selector(oa_class: str) -> Dict[str, str]:
    """
    Builds Selector for annotations that affect classes
    :param oa_class: The full jsonPath of the class being annotated (in block notation)
    :return: class selector object
    """
    return {
        AT_TYPE: "ods:ClassSelector",
        "ods:class": oa_class,
    }


def build_term_selector(ods_term: str) -> Dict[str, str]:
    """
    A selector for an individual Term
    :param ods_term: The full jsonPath of the term being annotated (in block notation)
    :return: term selector object
    """
    return {AT_TYPE: "ods:TermSelector", "ods:term": ods_term}


def build_fragment_selector(
    bounding_box: Dict[str, int], width: int, height: int
) -> Dict[str, Any]:
    """
    A selector for a specific Region of Interest (ROI). ROI is expressed in the AudioVisual Core
    standard: https://ac.tdwg.org/termlist/. This selector type is only applicable on media objects.
    :param bounding_box: object containing the bounding box of the ROI, in the following format,
    where values are expressed as pixels:
    {
        'x_min': 98,
        'y_min': 104,
        'x_max': 429,
        'y_max': 345
    },
    :param width: The width of the image, used to calculate the ROI
    :param height: the height of the image, used to calculate the ROI
    :return: Formatted fragment selector
    :raises: ValueError: if width or height is zero
    """
    if width <= 0 or height <= 0:
        raise ValueError(
            f"Invalid dimensions: width ({width}) and height ({height}) must be greater than zero."
        )

    return {
        AT_TYPE: "oa:FragmentSelector",
        "dcterms:conformsTo": "https://ac.tdwg.org/termlist/#711-region-of-interest-vocabulary",
        "ac:hasROI": {
            "ac:xFrac": bounding_box["x_min"] / width,
            "ac:yFrac": bounding_box["y_max"] / height,
            "ac:widthFrac": (bounding_box["x_max"] - bounding_box["x_min"]) / width,
            "ac:heightFrac": (bounding_box["y_max"] - bounding_box["y_min"]) / height,
        },
    }


def send_failed_message(job_id: str, message: str, producer: KafkaProducer) -> None:
    """
    Sends a failure message to the mas failure topic, mas-failed
    :param job_id: The id of the job
    :param message: The exception message
    :param producer: The Kafka producer
    """
    mas_failed = {"jobId": job_id, "errorMessage": message}
    producer.send("mas-failed", mas_failed)
