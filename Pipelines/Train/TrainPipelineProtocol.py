from typing import Protocol, List
from pyspark.sql import DataFrame

class TrainingPipelineProtocol(Protocol):
    """
    A protocol that outlines the required methods and properties for a training pipeline class.
    
    Attributes:
        feature_columns (List[str]): A list of column names to be used as features for the model.
        target (str): The name of the target variable column.
        train_split (float): The proportion of the dataset to be used for training.
        test_split (float): The proportion of the dataset to be used for testing.
        save_dir (str): The directory path where the trained model should be saved.
        overwrite (bool): A flag to indicate whether to overwrite the saved model if it exists.
    """

    feature_columns: List[str]
    target: str
    train_split: float
    test_split: float
    save_dir: str
    overwrite: bool

    def _prepare_data(self) -> None:
        """
        Prepares the data for training by splitting it into training and testing datasets,
        and transforming the features and target variables as needed.
        """
        ...

    def _configure_and_train_model(self) -> None:
        """
        Configures and trains the machine learning model using the prepared data.
        """
        ...

    def save_model(self) -> None:
        """
        Saves the trained model to a specified directory.
        """
        ...

    def load_and_evaluate_model(self) -> None:
        """
        Loads the trained model from the specified directory and evaluates its performance on the test dataset.
        """
        ...

    def process(self, df: DataFrame) -> None:
        """
        Executes the complete process of data preparation, model training, saving, and evaluation.
        
        Parameters:
            df (DataFrame): The Spark DataFrame containing the data to be used for training and testing the model.
        """
        ...
        
    def train(self, df: DataFrame) -> str:
        """
        Trains the model using the provided DataFrame.
        Returns the path to the saved model.
        
        Parameters:
            df (DataFrame): The Spark DataFrame containing the data to be used for training the model.
        """
        ...
