from DataSplit import DataSplit, DataSet
from DataLoader import DataLoader
from DataCleaner import DataCleaner
from DataAugmenter import DataAugmenter
from pyspark.sql import DataFrame
import logging
class PipeLine:
    def __init__(self, split: DataSplit):
        self.split = split
        self.data_loader = DataLoader(split)
        self.data = self.data_loader.load_data()
        
        
        # self.data_cleaner = DataCleaner()
        # self.data_augmenter = DataAugmenter()
        
                #  data_loader: DataLoader, 
                #  data_cleaner: DataCleaner, 
                #  data_augmenter: DataAugmenter):
        # self.data_loader = data_loader
        # self.data_data_cleaner = data_cleaner
        # self.data_augmenter = data_augmenter
        
    
        
    def run(self, predictions_only=False) -> DataFrame:
        if self.split == DataSplit.TRAIN:
            data = DataCleaner.clean_data(self.data)
            
        data = DataAugmenter.augment_data(self.data)
        if (self.split == DataSplit.VALIDATION or self.split == DataSplit.TEST) and not predictions_only:
            logging.warning("The submission files for validation and test data should contain predictions only. Consider setting predictions_only to True.")
        
        return data.df
    
    
        
        