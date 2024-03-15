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

The run.py script uses the configuration defined in config.yaml.

```bash
python run.py
```

# Adding a preprocessing step:

Adding a new preprocessing module to the pipeline consists of three steps:

1. Implement the module and ensure it conforms to the PreProcessingModuleProtocol.
2. Add the model to the PreprocessingModuleFactory so the factory can create the module at runtime.
3. Add the module to config.yaml



