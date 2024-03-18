from Pipelines.Preprocess.PreprocessingModuleFactory import PreprocessingModuleFactory
from Pipelines.Collect.DataCollectorFactory import DataCollectorFactory
from Pipelines.Train.TrainPipelineFactory import TrainPipelineFactory
from Pipelines.Predict.PredictPipelineFactory import PredictPipelineFactory
from Pipelines.Predict.PredictPipeline import PredictPipeline
import yaml
import os
import logging

from utils.initialize_modules_from_yaml import load_yaml_file, initialize_classes

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    logging.info("Starting the prediction pipeline...")

    try:
        # Load configuration
        logging.info("Loading prediction configuration...")
        config = load_yaml_file('predict_config.yaml')
        logging.info("✅ Prediction configuration loaded.")

        # Identify prediction files
        predict_data_dir = config["collect"]["config"]["data_dir"]
        prediction_files = [file for file in os.listdir(predict_data_dir) if file.endswith(".csv")]  # Adjusted to filter CSV files
        logging.info(f"Found {len(prediction_files)} files for prediction.")

        # Prepare for predictions
        predictions_dir = config["predict"]["save_dir"]
        predict_data_loader = DataCollectorFactory.create_module(config["collect"]["module"], config["collect"]["config"])
        logging.info("Data collector for prediction initialized.")

        # Collect prediction data
        logging.info("Collecting data for prediction...")
        dfs = predict_data_loader.collect_data()
        logging.info("✅ Data collected for prediction.")

        # Initialize prediction and preprocessing pipeline
        predict_pipeline = PredictPipeline(config["load_model"]["model_path"])
        predict_preprocessing_pipeline = initialize_classes(config["preprocess"])
        logging.info("Prediction and preprocessing pipelines initialized.")

        # Process and predict
        for source_file, df in zip(prediction_files, dfs):
            logging.info(f"Processing {source_file} for prediction...")
            for module in predict_preprocessing_pipeline:
                original_count = df.count()
                df = module.process(df)
                cleaned_count = df.count()
                diff = original_count - cleaned_count
                if diff > 0:
                    logging.error(f"🔴 Dropped {diff} rows from {source_file}. We should not drop rows when predicting.")
                    # raise error
                    raise ValueError("Dropped rows in prediction pipeline.")

            logging.info(f"Predicting outcomes for {source_file}...")
            predict_pipeline.predict(df, results_path=os.path.join(predictions_dir, f"preds_{source_file}"))
            logging.info(f"✅ Predictions saved for {source_file}.")

        logging.info("✅ Prediction pipeline completed successfully.")

    except Exception as e:
        logging.error(f"❌ Error in prediction pipeline: {e}")
        raise e
