
# ProML

<p align="center">
  ProML is a protocol-oriented machine learning pipeline designed to be reusable and extendable for various Big Data classification tasks. 
</p>

## 📋 **Overview**
Through a [protocol-driven design](https://scotteg.github.io/protocol-oriented-programming), ProML defines the blueprint for a full machine learning pipeline from data collection to inference. Out of the box, ProML implements a binary classification task of predicting whether a product review is useful or not through supervised learning on locally stored *.csv* files using [Pyspark](https://spark.apache.org/docs/latest/api/python/index.html). However, because of its modular and protocol-driven design, ProML can easily be adapted or extended to facilitate other inference tasks, schema changes, or even completely different kinds of data sources or frameworks like [DuckDB](https://duckdb.org).

## ✅ **Features**
- ### 🔄 *Protocol-driven design*
  In contrast to most traditional Python frameworks that typically use [Abstract Base Classes](https://docs.python.org/3/library/abc.html) to define modular and reusable interfaces, ProML uses [Python Protocols](https://mypy.readthedocs.io/en/stable/protocols.html), introduced in [PEP544](https://peps.python.org/pep-0544/), to do so. This approach ensures that any component in the pipeline can be easily replaced or extended without changing the core functionality, offering a flexible foundation for future developments.

- ### 📐 *Explicit Schema Declarations*
  "Explicit is better than implicit," is part of the [Zen of Python](https://peps.python.org/pep-0020/) and something ProML strides to fulfill. By using explicitly defined schemas for incoming data, ProML avoids a whole range of bugs and facilitates straightforward adaptation to future schema changes. 

- ### ✔️ *Explicit Data Quality Checks*
  ProML employs [Great Expectations](https://docs.greatexpectations.io/docs/reference/learn/conceptual_guides/gx_overview/) to ensure that data conforms to expectations at every step of the pipeline, enhancing reliability and trustworthiness of the results.

- ### ⚙️ *Configuration-driven Training*
  ProML's training process is driven by configuration files, allowing users to easily adjust model parameters, training data sources, and other settings without modifying the code. This flexibility simplifies experimentation and tuning of models for different tasks.

- ### 🧠 *Configuration-driven Inference*
  Similar to training, the inference process is controlled by configuration files, ensuring that models can be deployed and utilized with different parameters and in various environments efficiently and consistently.

- ### 🛠️ *Designed to be Adapted & Extended*
  The pipeline is built with adaptability and extensibility at its core, encouraging users to modify and extend it for their specific needs. Whether it's integrating new data sources, applying the pipeline to different machine learning tasks, or enhancing its capabilities with additional features, ProML is designed to support and streamline these processes.

## ⚠️ **Known Issues**
To be added as encountered and documented.

## 📦 **Requirements & Installation**

ProML uses Poetry as its dependency manager. If you do not have Poetry installed, follow [Poetry's installation instructions](https://python-poetry.org/docs/). Once you have Poetry installed, you can install the dependencies defined in *pyproject.toml* and activate the corresponding virtual environment by running the following commands:

> [!WARNING]
> MacOS users might run into problems with Java when trying to install the dependencies. If you encounter any issues, consider downloading and installing the ARM version of Java8 at: https://www.java.com/en/download/

```bash
# install dependencies defined in pyproject.toml
poetry install
```

```bash
# activate shell
poetry shell
```

## 🚀 **Quick Start**
Offer a simple example or tutorial that shows how to get the pipeline up and running with minimal setup. This could be a basic example that demonstrates the pipeline's core functionality.

## 🏛️ **Architecture**
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