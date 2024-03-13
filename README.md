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


# Augment:

To improve training / prediction performance, consider the following steps:

1. Add a new data augmentation method to DataAugmenter.py
2. Make sure to add the new feature to the feature list in the RandomForest algorithm in train.ipynb.


# Train:

The train.ipynb noteboko connects two pipelines to train and save a model:

(1) First, it uses our own pipeline (Pipeline.py) to preprocess the data.
(2) Then it uses the PySpark's built-in pipeline functionality to transform the features into a feature vector and
run a RandomForests model on the data to predict the label.

The trained model is saved under the 'model' dir.




# Predict

Once a model is saved under the 'model' dir, we can run the predict.ipynb notebook to run the model
on the validation and test set and create the corresponding submission files under the 'predictions' dir.
