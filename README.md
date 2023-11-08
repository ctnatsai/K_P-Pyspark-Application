# KommatiParaDataPrepApp

The PySpark application performs data preparation tasks on two input datasets and produces an output dataset. The application follows the following steps:

1. Read the input datasets from CSV files using the SparkSession object and the file paths specified in the configuration file.
2. Stage the input datasets by applying some cleaning operations, such as filtering and dropping columns, based on the parameters specified in the configuration file.
3. Integrate the staged datasets by joining them on a common column (`id`) and removing the duplicate column.
4. Transform the integrated dataset by renaming some columns based on the parameters specified in the configuration file. You can also add more transformation logic here if needed.
5. Write the output dataset to a desired location or format. In this example, the output dataset is simply appending data to the output datafile without considerations on merge conditions.

## Configuration file

The application uses a YAML configuration file to store the parameters for the data preparation tasks. The configuration file has the following structure:

```yaml
input_dataset_one_config:
  input_filepath: The file path of the first input dataset
  filter_column: The column name to apply the filter on
  filter_criteria: The list of values to keep in the filter column
  drop_columns: The list of columns to drop from the first input dataset

input_dataset_two_config:
  input_filepath: The file path of the second input dataset
  filter_column: The column name to apply the filter on
  filter_criteria: The list of values to keep in the filter column
  drop_columns: The list of columns to drop from the second input dataset

output_dataset_config:
  rename_columns: A dictionary of old and new column names to rename in the output dataset
  #other parameters
```

## How to run the application
To run the application, you need to have PySpark installed and configured on your system. You also need to have the 
input datasets and the configuration file in the same directory as the application script. Then, you can run the 
following command in the terminal:

```bash
spark-submit KommatiParaDataPrepApp.py
```

This will execute the application and display the output dataset. You can also modify the application script to write 
the output dataset to a different location or format if you want.

## Docstrings

The application script also contains docstrings for each class and method, using the reStructuredText (reST) format. 
The docstrings provide a brief description of the purpose and functionality of each class and method, as well as the 
parameters and return values. The docstrings can be used to generate documentation for the application using tools like 
Sphinx or PyDoc. Here is an example of a docstring for the read method:

```python
def read(self, spark_session, config_dict):
    """
    This method reads the input datasets from CSV files using the SparkSession object and the file paths specified in 
    the configuration file. It returns a dictionary of the input datasets.

    Parameters
    ----------
    :param spark: The SparkSession object
    :type spark: pyspark.sql.SparkSession
    :param config_dict: The configuration dictionary
    :type config_dict: dict

    Returns
    -------
    :return: A dictionary of the input datasets
    :rtype: dict
    """
```
## Testing

This is a PySpark project that uses pytest to test the data preparation tasks performed by the KommatiParaDataPrepApp 
class. The project uses the chispa library to compare Spark dataframes and assert their equality.

### Installation

```bash
pip install chispa
pip install pytest
```

### Test cases

The project contains one test module, test_kommatiparadataprepapp.py, that defines the following test cases:

1. test_rename_columns: This test case checks if the rename_columns method of the KommatiParaDataPrepApp class correctly 
renames the columns of a given dataframe according to a configuration dictionary. The test case uses a sample dataframe 
of financial information and a configuration dictionary that specifies the new column names. The test case asserts the 
equality of the actual and expected dataframes using the assert_df_equality function from chispa.

2. test_integrate: This test case checks if the integrate method of the KommatiParaDataPrepApp class correctly joins 
two input dataframes on a common column (id) and removes the duplicate column. The test case uses two sample dataframes 
of client and financial information and a configuration dictionary that specifies the output schema. The test case 
asserts the equality of the actual and expected dataframes using the assert_df_equality function from chispa.

3. test_filter: This test case checks if the filter method of the KommatiParaDataPrepApp class correctly filters a given
dataframe based on a column name and a list of filter criteria. The test case uses a sample dataframe of client 
information and a configuration dictionary that specifies the filter column and criteria. The test case asserts the 
equality of the actual and expected dataframes using the assert_df_equality function from chispa.

4. test_clean: This test case checks if the clean method of the KommatiParaDataPrepApp class correctly applies the 
filter method and drops some columns from a given dataframe. The test case uses a sample dataframe of client information 
and a configuration dictionary that specifies the filter column, criteria, and drop columns. The test case asserts the 
equality of the actual and expected dataframes using the assert_df_equality function from chispa.

### How to run the tests

To run the tests, you need to have the test_kommatiparadataprepapp.py module and the kommatiparadataprepapp.py module in 
the same directory. Then, you can run the following command in the terminal:

````python
pytest test_kommatiparadataprepapp.py
````

This will execute the tests and display the results. You can also use the -v option to get more verbose output.

## Logging
This is a Python module that defines the logging configuration and the logger object for the KommatiParaDataPrepApp, a 
PySpark application that performs data preparation tasks on two input datasets and produces an output dataset.

### Logging configuration

The logging module is stored in the logs_config directory in the root directory of the project. 

The logging configuration defines the following parameters:

1. The name of the logger object: ‘KommatiParaDataPrepApp’
2. The log level of the logger object: logging.INFO, which means that only messages with INFO level or higher will be logged.
3. The file handler object: file_handler, which writes the log messages to the app.log file in the logs_config directory, 
rotates the file when it reaches 1 MB, and keeps the latest 10 log files.
4. The formatter object: formatter, which formats the log messages with the date and time, the name of the logger, 
the level of the message, and the message itself.

## Packaging
