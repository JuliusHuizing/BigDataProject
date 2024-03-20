# BD-Project

## Install dependencies:

for MacOS users, ensure you donwload and install the arm version of java8: https://www.java.com/en/download/

If you do not have poetry alread, install [poetry](https://python-poetry.org/docs/).

```bash
# install dependencies defined in pyproject.toml
poetry install
# activate shell
poetry shell

```

## Add dependencies:
```bash
poetry add <dependency_name>
```

# Train & predict

The train.py script uses the configuration defined in train_config.yaml to train, evaluate, and save a model:

```bash
python train.py
```

The predict.py script uses predict_config.yaml to predict labels for new datapoints.

```bash
python predict.py
```

# Adding a preprocessing step:

Adding a new preprocessing module to the pipeline consists of three steps:

1. Implement the module and ensure it conforms to the PreProcessingModuleProtocol.
2. Add the model to the PreprocessingModuleFactory so the factory can create the module at runtime.
3. Add the module to train_config.yaml and predict_config.yaml where appropiate.



