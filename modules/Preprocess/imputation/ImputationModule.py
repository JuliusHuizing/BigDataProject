from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, collect_list, rand
from pyspark.sql.types import StringType
from pyspark.sql.functions import mean as _mean
import random

class ImputationModule:
    def __init__(self, column_name: str, imputation_method: str = "mean", imputation_value: str = None):
        """
        Initializes the imputation module to handle missing values in a specified column
        using various imputation methods.

        Parameters:
        - column_name: The name of the column to impute missing values in.
        - imputation_method: The method of imputation ('mean', 'median', 'mode', 'constant', 'random'). Defaults to "mean".
        - imputation_value: The value used for 'constant' imputation. Required if imputation_method is 'constant'.
        """
        self.column_name = column_name
        self.imputation_method = imputation_method
        self.imputation_value = imputation_value

    def process(self, data: DataFrame) -> DataFrame:
        """
        Processes the input DataFrame to impute missing values in the specified column using the chosen method.

        Parameters:
        - data: A PySpark DataFrame containing the data to process.

        Returns:
        - A PySpark DataFrame with imputed values in the specified column.
        """
        if self.imputation_method == "constant" and self.imputation_value is None:
            raise ValueError("Imputation value must be provided for constant imputation method.")

        # Impute missing values based on the specified method
        if self.imputation_method == "mean":
            # For numerical columns, replace missing values with the mean
            avg_value = data.filter(col(self.column_name).isNotNull()).agg(_mean(col(self.column_name))).collect()[0][0]
            impute_expr = when(col(self.column_name).isNull(), lit(avg_value)).otherwise(col(self.column_name))
        elif self.imputation_method == "constant":
            impute_expr = when(col(self.column_name).isNull(), lit(self.imputation_value)).otherwise(col(self.column_name))
        elif self.imputation_method == "random":
            # Collect non-null instances and randomly select one for imputation
            non_null_samples = data.filter(col(self.column_name).isNotNull()).select(self.column_name).collect()
            sample_values = [row[self.column_name] for row in non_null_samples]
            impute_expr = when(col(self.column_name).isNull(), lit(random.choice(sample_values))).otherwise(col(self.column_name))
        else:
            raise NotImplementedError(f"Imputation method '{self.imputation_method}' is not supported.")

        return data.withColumn(self.column_name, impute_expr)
