from pyspark.sql import DataFrame
import logging
class DataChecker:
    
    def hard_checks(self, df: DataFrame):
        # assert the dataframe is not empty
        assert df.count() > 0, "The dataframe is empty"
        # # assert there are no null values:
        # assert df.filter(df.isnull()).count() == 0, "There are null values in the dataframe"
        # assert there are no duplicate rows
        assert df.count() == df.dropDuplicates().count(), "There are duplicate rows in the dataframe"
        # assert the label column has exactly two unique values
        unique_values = df.select("label").distinct().collect()
        assert len(df.select("label").distinct().collect()) == 2, f"Label column must have exactly two unique values, but has {len(unique_values)}."
    
    def soft_checks(self, df: DataFrame):
        pass
        
    def check_data(self, df: DataFrame):
        try: 
            self.hard_checks(df)
        except Exception as e:
            logging.error("Hard checks failed")
            # throw error
            raise e
        
        try:
            self.soft_checks(df)
        except Exception as e:
            logging.warning("⚠️ Soft checks failed: Continuing, but training performance may be impaired.")
            # log warning
            
    def __init__(self):
        pass