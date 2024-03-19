
from .DataQualityCheckModuleProtocol import DataQualityCheckModule
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.data_context import DataContext
import great_expectations as gx

class GXDataQualityCheckModule(DataQualityCheckModule):
        
    def __init__(self, expectation_suite_name: str, continue_after_failure: bool):
        self.context = DataContext("gx/")
        self.expectation_suite_name = expectation_suite_name
        self.continue_on_failure = continue_after_failure
        datasource = self.context.sources.add_or_update_spark("my_spark_datasource")
        # _ = datasource.delete_asset("meh")
        self.data_asset = datasource.add_dataframe_asset("meh")

    def _check(self, data: DataFrame) -> bool:
        batch_request = self.data_asset.build_batch_request(dataframe=data)
        self.checkpoint = self.context.add_or_update_checkpoint(
        name="checkpoint",
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": f"{self.expectation_suite_name}",
            },
        ],
        )
        checkpoint_result = self.checkpoint.run()
        return checkpoint_result["success"] == True
        

    def process(self, data: DataFrame) -> DataFrame:
        if not self._check(data) and not self.continue_on_failure:
            raise ValueError("Data quality check failed")
        else:
            return data
