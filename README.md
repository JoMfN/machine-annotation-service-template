# Machine Annotation Service Template
Thank you for your interest in developing Machine Annotation Services (MASs) for DiSSCo!

This repository contains Boilerplate code to facilitate development of MASs. For more information, see our [MAS Developers Guide](https://dissco.github.io/mas-developers-documentation/)
You can find example MASs [on our GitHub](https://github.com/DiSSCo/demo-enrichment-service-image/)


## Getting Help
Creating issues in this repository is the best way to receive a quick response from the DiSSCo development team.

## Using This Repository
This repository is intended to be forked and used as a template for the development of MASs. 
The `annotation` package contains code that will format resulting calculations to the openDS annotation model.
Two templates are provided: a default template and a batch template. Use the batch template if you wish to support batch operations. \
Supporting batching may result in lower computational demand and reduce workload for the MAS and associated systems, but it 
requires careful work to set up. More information can be found on our [wiki](https://github.com/DiSSCo/dissco-developers-documentation/wiki/Information-for-Machine-Annotation-Service-(MAS)-Developers). 
If your MAS does not support batching, the default template is more suitable.

## Kafka Message

Messages are sent between DiSSCo and MASs using [Kafka](https://kafka.apache.org/), an asynchronous event messaging platform. 

The incoming message will be in the following format: 
```
{
    "object": { ... },
    "jobID": "8a325743-bf32-49c7-b3a1-89e738c37dfc",
    "batchingRequested": true
}
```
Where `object` is the Digital Specimen or Digital Media in openDS, `jobID` is a UUID that must be passed back to DiSSCo, and `batchingRequested` is an optional parameter indicating that the user has requested batching on the scheduled annotation. A MAS must be properly configured to batch annotations. See the wiki entry for more information on batching annotations. 

## Data Model

The following table contains references to relevant schemas and a human-readable Terms reference.

| Resource                   | JSON Schema                                                                                                      | Terms Site                                                                                                           |
|----------------------------|------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| Digital Specimen           | [Schema](https://schemas.dissco.tech/schemas/fdo-type/digital-specimen/0.4.0/digital-specimen.json)              | [Terms](https://terms.dissco.tech/digital-specimen-terms)                                                            |
| Digital Media              | [Schema](https://schemas.dissco.tech/schemas/fdo-type/digital-media/0.4.0/digital-media.json)                    | [Terms](https://terms.dissco.tech/digital-media-terms)                                                               |
| Annotation Event to DiSSCo | [Schema](https://schemas.dissco.tech/schemas/developer-schema/annotation/0.4.0/annotation-processing-event.json) | [Terms](https://terms.dissco.tech/annotation-terms) (Note: contains terms computed by DiSSCo as well as the MAS) |

The resulting message back to DiSSCo must comply to the `Annotation Event to DiSSCo` schema.
