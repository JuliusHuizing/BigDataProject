
from Pipelines.Preprocess.PreprocessingPipeline import PreprocessingPipeline
from Pipelines.Preprocess.PreprocessingConfig import PreprocessingConfig
from Pipelines.Train.TrainPipeline import TrainPipeline

if __name__ == "__main__":
    preprocessing_pipeline = PreprocessingPipeline (
        data_collector=PreprocessingConfig.DATA_COLLECTOR,
        preprocessing_modules=PreprocessingConfig.PREPROCESSING_MODULES
    )
    
    processed_df = preprocessing_pipeline.run()
    
    training_pipeline = TrainPipeline(df=processed_df)
    training_pipeline.prepare_data()
    training_pipeline.configure_and_train_model()
    training_pipeline.load_and_evaluate_model()
    
    
