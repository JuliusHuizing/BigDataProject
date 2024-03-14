from pyspark.sql import DataFrame

class CleanDuplicateRows:
    
    def process(self, data: DataFrame) -> DataFrame:
        """
        Removes duplicate rows from a PySpark DataFrame.

        Parameters:
        - df: The PySpark DataFrame to process.

        Returns:
        - A PySpark DataFrame with duplicate rows removed.
        """
        initial_count = data.count()
        data = data.dropDuplicates()
        final_count = data.count()
        print(f"Removed {initial_count - final_count} duplicate rows.")
        return data
