
from Pipelines.Preprocess.PreprocessingPipeline import PreprocessingPipeline
from Pipelines.Preprocess.PreprocessingConfig import PreprocessingConfig

if __name__ == "__main__":
    pipeline = PreprocessingPipeline(data_collector=PreprocessingConfig.DATA_COLLECTOR,
                      preprocessing_modules=PreprocessingConfig.PREPROCESSING_MODULES)
    processed_df = pipeline.run()
    processed_df.show()
    
