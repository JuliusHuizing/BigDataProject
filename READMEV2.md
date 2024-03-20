
# ProML

<p align="center">
  ProML is protocol-oriented machine learning pipeline designed to be reusable and extendable for various Big Data classification tasks. 
</p>

## 📋 **Overview**
Through a [protocol-driven design](https://scotteg.github.io/protocol-oriented-programming), ProML defines the blueprint for a full machine learning pipeline from data collection to inference. Out of the box, ProML implements a binary classification task of prediciting wether a product review is usefull or not through supervised learning on locally stored *.csv* files using [Pyspark](https://spark.apache.org/docs/latest/api/python/index.html). However, because of its modular and protocol-driven design, ProML can easily be adapted or extended to faciliate other inference tasks, schema changes, or even completely different kinds of datasources or frameworks like [DuckDB](https://duckdb.org).

## ✅ **Features**
- Protocol-driven design
- Configuration-driven training
- declared vs 
- Reusable modules
- 

## ⚠️ Known Issues**
#TODO:


## **Requirements & Installation**
ProML uses Poetry as dependency manager. If you do not have Poetry installed, follow [Poetry's installation instructions](https://python-poetry.org/docs/). Once you have Poetry installed, you can install the dependencies defined in *pyproject.toml* and activate the correseponding virtual environment by running the following commands:

> [!WARNING]
> MacOS' users might run into problems with Java when trying to install the dependencies. If you run into any problems, consider downloading and installing the ARM version of Java8 at: http
s://www.java.com/en/download/

```bash
# install dependencies defined in pyproject.toml
poetry install
# activate shell
poetry shell
```


## **Quick Start**
Offer a simple example or tutorial that shows how to get the pipeline up and running with minimal setup. This could be a basic example that demonstrates the pipeline's core functionality.

## **Architecture**
Describe the architecture of your machine learning pipeline. Include:

A high-level overview of the components and how they interact.
The role of protocols in ensuring reusability and extensibility.
Diagrams or flowcharts, if they can help in understanding the architecture better.
Extending the Pipeline
Provide a detailed guide on how users can extend your pipeline by:

## Handling Additional Labeled Data
#TODO: 

## **Handling Schema Changes**
#TODO

## **Reusing / Adapting the pipeline for other ML tasks

<!-- Implementing new protocols for additional functionalities.
Adding new components or models to the pipeline.
Modifying existing components to suit specific needs.
Contributing
Encourage contributions and outline how others can contribute to your project. Include:

Guidelines for submitting issues or bugs.
Instructions for proposing enhancements or new features.
The process for submitting pull requests.
License
Specify the license under which your pipeline is released, ensuring users understand how they can use or modify it. -->

<!-- ## Contact
Offer ways for users to get in touch with you for further questions or collaborations. This could include email, a project mailing list, or links to project forums. -->