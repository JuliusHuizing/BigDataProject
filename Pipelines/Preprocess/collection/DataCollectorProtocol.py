from pyspark.sql import DataFrame
from typing import Protocol


class DataCollectorProtocol(Protocol):
    """
    A protocol for classes responsible for collecting data.
    Implementations might include, but are not limited to,
    fetching data from APIs or databases (fetchers) and loading data from
    local files or in-memory structures (loaders).

    The primary purpose of conforming classes is to abstract away the details
    of how data is sourced and to provide a consistent interface for retrieving
    data as a DataFrame, ready for further processing or analysis.
    
    Methods:
        loadData: Retrieves data and returns it as a DataFrame. The specifics
                  of data retrieval (remote or local) are left to the implementing
                  class.
    """
    
    def collect_data(self) -> DataFrame:
        pass