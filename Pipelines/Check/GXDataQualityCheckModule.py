
from .DataQualityCheckModuleProtocol import DataQualityCheckModule
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.data_context import DataContext
import great_expectations as gx
import logging
import webbrowser
import os
class GXDataQualityCheckModule(DataQualityCheckModule):
        
    def __init__(self, expectation_suite_name: str, continue_after_failure: bool):
        self.context = DataContext("gx/")
        self.expectation_suite_name = expectation_suite_name
        self.continue_on_failure = continue_after_failure
        datasource = self.context.sources.add_or_update_spark("spark_data_source")
        self.data_asset = datasource.add_dataframe_asset("asset")

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
        if not self._check(data):
            if self.continue_on_failure:
                logging.warning("🟠 Data quality check failed, but continuing according to config.")
                return data
            else:
                # FIXME: this require absolute path...
                # url = 'file:///../../gx/uncommited/data_docs/index.html'
                dirname = os.path.dirname(__file__)
                url = 'file://' + os.path.join(dirname, '../../gx/uncommitted/data_docs/local_site/index.html')
                webbrowser.open(url, new=2)

                raise ValueError("Data quality check failed")   
            
        else:
            return data
