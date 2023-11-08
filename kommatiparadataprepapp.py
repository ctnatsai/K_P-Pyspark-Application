import sys
import yaml
import pandas as pd

from pyspark.sql import SparkSession
from logs_config import logger_app


class KommatiParaDataPrepApp:
    """A PySpark application that performs data preparation tasks on two input datasets and produces an output dataset.

        This class defines the methods for reading, staging, integrating, transforming, and writing the datasets, as
        well as the main method for running the application. It also takes a configuration dictionary as an argument
        to initialize the class.

        :param config_dict: The configuration dictionary
        :type config_dict: dict
        """

    def write(self, dataset):
        """Write the output dataset to a desired location or format.

                This method writes the output dataset to a desired location or format. In this example, the output
                dataset is written to output file "client_data/output.csv". You can modify this method to write the output
                dataset to a different location or format if you want.

                :param dataset: The output dataset
                :type dataset: pyspark.sql.DataFrame
                """

        # Log the start of the write method
        logger_app.logger.info('Writing the output dataset')
        print("Writing the output dataset")

        # Write the output dataset to a CSV file as pandas dataframe

        dataset.toPandas().to_csv("client_data/output.csv", index=False)

        # Log the end of the write method
        logger_app.logger.info('Output dataset written successfully')
        print("Output dataset written successfully")


    def rename_columns(self,dataset, config_dict):
        """Rename some columns in the dataset based on the configuration file.

                This method renames some columns in the dataset based on the configuration file. It takes a dictionary
                of old and new column names as an argument and returns the dataset with the renamed columns.

                :param dataset: The dataset to rename columns
                :type dataset: pyspark.sql.DataFrame
                :param config_dict: The configuration dictionary
                :type config_dict: dict
                :return: The dataset with the renamed columns
                :rtype: pyspark.sql.DataFrame
                """

        # Log the start of the rename_columns method
        logger_app.logger.info('Renaming columns in the dataset')
        print("Renaming columns in the dataset")

        # Get rename columns
        rename_columns = config_dict['output_dataset_config']['rename_columns']

        for key, value in rename_columns.items():
            dataset = dataset.withColumnRenamed(key, value)

        # Log the end of the rename_columns method
        logger_app.logger.info('Columns renamed successfully')
        print("Columns renamed successfully")

        return dataset

    def transform(self, dataset, config_dict):
        """Transform the dataset by applying some logic.

                This method transforms the dataset by applying some logic. In this example, the only logic applied is
                renaming some columns based on the configuration file. You can also add more transformation logic here
                if needed. It returns the transformed dataset.

                :param dataset: The dataset to transform
                :type dataset: pyspark.sql.DataFrame
                :param config_dict: The configuration dictionary
                :type config_dict: dict
                :return: The transformed dataset
                :rtype: pyspark.sql.DataFrame
                """

        # Log the start of the transform method
        logger_app.logger.info('Transforming the dataset')
        print("Transforming the dataset")

        # Rename columns
        dataset = self.rename_columns(dataset, config_dict)

        # Perform other transformations

        # Log the end of the transform method
        logger_app.logger.info('Dataset transformed successfully')
        print("Dataset transformed successfully")

        return dataset

    def integrate(self, datasets):
        """Integrate the two input datasets by joining them on a common column.

                This method integrates the two input datasets by joining them on a common column (`id`) and removing
                the duplicate column. It returns the integrated dataset.

                :param datasets: A dictionary of the two input datasets
                :type datasets: dict
                :return: The integrated dataset
                :rtype: pyspark.sql.DataFrame
                """

        # Log the start of the integrate method
        logger_app.logger.info('Integrating the two input datasets')
        print("Integrating the two input datasets")

        # Get datasets
        dataset_one = datasets.get('dataset1')
        dataset_two = datasets.get('dataset2')

        # Join datasets on id a nd remove one of the id columns
        joined_datasets = (dataset_one.join(dataset_two, dataset_one.id == dataset_two.id, 'inner')
                           .drop(dataset_two.id))

        # Log the end of the integrate method
        logger_app.logger.info('Datasets integrated successfully')
        print("Datasets integrated successfully")

        return joined_datasets

    def filter(self, dataset, column_name, filter_criteria):
        """Filter the dataset by keeping only the values that match the filter criteria in the filter column.

                This method filters the dataset by keeping only the values that match the filter criteria in the
                filter column. It takes the column name and the filter criteria as arguments and returns the
                filtered dataset. If the filter criteria is None or the column name is not in the dataset,
                it returns the original dataset.

                :param dataset: The dataset to filter
                :type dataset: pyspark.sql.DataFrame
                :param column_name: The column name to apply the filter on
                :type column_name: str
                :param filter_criteria: The list of values to keep in the filter column
                :type filter_criteria: list
                :return: The filtered dataset
                :rtype: pyspark.sql.DataFrame
                """

        # Log the start of the filter method
        logger_app.logger.info('Filtering the dataset based on column {} and criteria {}'.format(column_name, filter_criteria))
        print("Filtering the dataset based on column {} and criteria {}".format(column_name, filter_criteria))

        filtered_dataset = dataset
        # Only filter if filter_criteria is not empty and column_name is in dataset
        if column_name in dataset.columns and filter_criteria is not None:
            # Filter dataset
            filtered_dataset = dataset.filter(getattr(dataset, column_name).isin(filter_criteria))

            # Log the end of the filter method
            logger_app.logger.info('Dataset filtered successfully')
            print("Dataset filtered successfully")

        return filtered_dataset

    def clean(self, dataset, filter_column, filter_criteria, drop_columns):
        """Clean the dataset by applying some operations, such as filtering and dropping columns.

                This method cleans the dataset by applying some operations, such as filtering and dropping columns.
                It takes the filter column, the filter criteria, and the list of columns to drop as arguments and
                returns the cleaned dataset.

                :param dataset: The dataset to clean
                :type dataset: pyspark.sql.DataFrame
                :param filter_column: The column name to apply the filter on
                :type filter_column: str
                :param filter_criteria: The list of values to keep in the filter column
                :type filter_criteria: list
                :param drop_columns: The list of columns to drop from the dataset
                :type drop_columns: list
                :return: The cleaned dataset
                :rtype: pyspark.sql.DataFrame
                """

        # Log the start of the clean method
        logger_app.logger.info('Cleaning the dataset')
        print("Cleaning the dataset")

        # Filter dataset
        filtered_dataset = self.filter(dataset, filter_column, filter_criteria)

        # Drop columns
        with_dropped_columns = filtered_dataset.drop(*drop_columns)

        # Log the end of the clean method
        logger_app.logger.info('Dataset cleaned successfully')
        print("Dataset cleaned successfully")

        return with_dropped_columns

    def stage(self, datasets, config_dict):
        """Stage the input datasets by applying some cleaning operations.

                This method stages the input datasets by applying some cleaning operations, such as filtering and
                dropping columns, based on the parameters specified in the configuration file. It returns a dictionary
                of the staged datasets.

                :param datasets: A dictionary of the input datasets
                :type datasets: dict
                :param config_dict: The configuration dictionary
                :type config_dict: dict
                :return: A dictionary of the staged datasets
                :rtype: dict
                """

        # Log the start of the stage method
        logger_app.logger.info('Staging the input datasets')
        print("Staging the input datasets")

        # Get datasets
        dataset_one = datasets.get('dataset1')
        dataset_two = datasets.get('dataset2')

        # Clean up dataset_one
        dataset_one = self.clean(dataset_one, config_dict['input_dataset_one_config']['filter_column'],
                                 config_dict['input_dataset_one_config']['filter_criteria'],
                                 config_dict['input_dataset_one_config']['drop_columns'])

        # Clean up dataset_two
        dataset_two = self.clean(dataset_two, config_dict['input_dataset_two_config']['filter_column'],
                                 config_dict['input_dataset_two_config']['filter_column'],
                                 config_dict['input_dataset_two_config']['drop_columns'])

        # Log the end of the stage method
        logger_app.logger.info('Datasets staged successfully')
        print("Datasets staged successfully")

        return {'dataset1': dataset_one, 'dataset2': dataset_two}

    def read(self, spark, config_dict):
        """Read the input datasets from CSV files.

                This method reads the input datasets from CSV files using the SparkSession object and the file paths
                /;specified in the configuration file. It returns a dictionary of the input datasets.

                :param spark: The SparkSession object
                :type spark: pyspark.sql.SparkSession
                :param config_dict: The configuration dictionary
                :type config_dict: dict
                :return: A dictionary of the input datasets
                :rtype: dict
                """

        # Log the start of the read method
        logger_app.logger.info('Reading the input datasets')
        print("Reading the input datasets")

        # Get file paths
        dataset_one_filepath = config_dict['input_dataset_one_config']['input_filepath']
        dataset_two_filepath = config_dict['input_dataset_two_config']['input_filepath']

        # Read datasets
        dataset_one = spark.read.format("csv").option("header", "true").load(dataset_one_filepath)
        dataset_two = spark.read.format("csv").option("header", "true").load(dataset_two_filepath)

        # Log the end of the read method
        logger_app.logger.info('Datasets read successfully')
        print("Datasets read successfully")

        return {'dataset1': dataset_one, 'dataset2': dataset_two}

    def run(self):
        """Run the application by executing the methods in the following order: read, stage, integrate, transform, and write.

                This method runs the application by executing the methods in the following order: read, stage, integrate,
                transform, and write. It creates a SparkSession object and passes it to the read method. It also passes
                the configuration dictionary to the other methods. It displays the output dataset using the show()
                method.
                """

        # Log the start of the run method
        logger_app.logger.info('Running the application')
        print("Running the application")

        # Create spark session
        spark = SparkSession.builder.appName("KommatiParaDataPrepApp").getOrCreate()

        read_datasets = self.read(spark, self.config_dict)

        stage_datasets = self.stage(read_datasets, self.config_dict)

        integrate_dataset = self.integrate(stage_datasets)

        transform_dataset = self.transform(integrate_dataset, self.config_dict)

        # Write to output
        self.write(transform_dataset)

        # Log the end of the run method
        logger_app.logger.info('Application run successfully')
        print("Application run successfully")

    def __init__(self, config_dict):
        self.config_dict = config_dict

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # Load config files
    config_file_path = 'config_files/client_financial_info_config.yaml'
    with open(config_file_path, 'r') as yml:
        config_dict = yaml.full_load(yml)

    # Run application
    try:
        app = KommatiParaDataPrepApp(config_dict)
        app.run()
    except Exception as e:
        # Exception Handler: Print Error message and exit
        print(f"An error occurred: {e}")
        sys.exit(1)
