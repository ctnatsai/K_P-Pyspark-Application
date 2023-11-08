import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from kommatiparadataprepapp import KommatiParaDataPrepApp

# Define spark fixture
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("pytest-pyspark").getOrCreate()
    yield spark
    spark.stop()

def test_rename_columns(spark):
    # Create test data
    financial_info_data = [(1, "1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2", "visa-electron", 4175006996999270),
                           (8, "1Q5FAwgXbhRxP1uYpgwXMY67zFzUDkwutW", "mastercard", 5100174550682620),
                           (19, "1FeH4KecDLZYXEcAut4Dj4ciojsBEB1pDb", "mastercard", 5100176279014886)]

    financial_info_df = spark.createDataFrame(financial_info_data, ["id", "btc_a", "cc_t", "cc_n"])

    # Define test configuration
    config_dict = {
        "output_dataset_config": {
            "rename_columns": {
                "id": "client_identifier",
                "btc_a" : "bitcoin_address",
                "cc_t" : "credit_card_type",
                "cc_n" : "credit_card_number"
            }
        }
    }

    # Create instance of KommatiParaDataPrepApp
    app = KommatiParaDataPrepApp(config_dict)

    # Call rename_columns method
    result = app.rename_columns(financial_info_df, config_dict)

    # Define expected result
    expected_data = [(1, "1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2", "visa-electron", 4175006996999270),
                           (8, "1Q5FAwgXbhRxP1uYpgwXMY67zFzUDkwutW", "mastercard", 5100174550682620),
                           (19, "1FeH4KecDLZYXEcAut4Dj4ciojsBEB1pDb", "mastercard", 5100176279014886)]

    expected_df = spark.createDataFrame(expected_data, ["client_identifier", "bitcoin_address", "credit_card_type", "credit_card_number"])

    # Check if dataframes are equal
    assert_df_equality(result, expected_df)


def test_integrate(spark):
    # Create client test data
    client_data = [(1, "Feliza", "Eusden", "feusden0@ameblo.jp", "France"),
                   (8,"Derk", "Mattielli", "dmattielli7@slideshar.net", "United States"),
                   (18, "Richard", "Drinan","rdrinanh@odnoklassniki.ru", "United Kingdom")]

    # Create financial info test data
    financial_info_data = [(1, "1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2", "visa-electron", 4175006996999270),
                           (8, "1Q5FAwgXbhRxP1uYpgwXMY67zFzUDkwutW", "mastercard", 5100174550682620),
                           (19, "1FeH4KecDLZYXEcAut4Dj4ciojsBEB1pDb", "mastercard", 5100176279014886)]

    # Create spark dataframes
    client_df = spark.createDataFrame(client_data, ["id", "first_name", "last_name", "email", "country"])
    financial_info_df = spark.createDataFrame(financial_info_data, ["id", "btc_a", "cc_t", "cc_n"])

    # Define test configuration
    config_dict = {
        "output_dataset_config": {
            "source_schema": {
                "client_id": "int",
                "email": "string",
                "country": "string",
                "bitcoin_address": "string",
                "credit_card_type": "string"
            }
        }
    }

    # Create instance of KommatiParaDataPrepApp
    app = KommatiParaDataPrepApp(config_dict)

    # Define test datasets
    datasets = {
        "dataset1": client_df,
        "dataset2": financial_info_df
    }

    # Call integrate method
    result = app.integrate(datasets)

    # Define expected result
    expected_data = [(1, "Feliza", "Eusden", "feusden0@ameblo.jp", "France",
                      "1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2", "visa-electron", 4175006996999270),
                   (8, "Derk", "Mattielli", "dmattielli7@slideshar.net", "United States",
                    "1Q5FAwgXbhRxP1uYpgwXMY67zFzUDkwutW", "mastercard", 5100174550682620)]

    expected_df = spark.createDataFrame(expected_data, ["id", "first_name", "last_name", "email", "country",
                                                        "btc_a", "cc_t", "cc_n"])

    # Check if dataframes are equal
    assert_df_equality(result, expected_df)

def test_filter(spark):
    # Create test data
    client_data = [(1, "Feliza", "Eusden", "feusden0@ameblo.jp", "France"),
                   (8, "Derk", "Mattielli", "dmattielli7@slideshar.net", "United States"),
                   (18, "Richard", "Drinan", "rdrinanh@odnoklassniki.ru", "United Kingdom")]

    client_df = spark.createDataFrame(client_data, ["id", "first_name", "last_name", "email", "country"])

    # Define test configuration
    config_dict = {
        "input_dataset_one_config": {
            "filter_column" : "country",
            "filter_criteria" : ["United Kingdom", "Netherlands"]
        }
    }

    # Create instance of KommatiParaDataPrepApp
    app = KommatiParaDataPrepApp(config_dict)

    # Define test parameters
    column_name = config_dict['input_dataset_one_config']['filter_column']
    filter_criteria = config_dict['input_dataset_one_config']['filter_criteria']

    # Call filter method
    result = app.filter(client_df, column_name, filter_criteria)

    # Define expected result
    expected_data = [(18, "Richard", "Drinan", "rdrinanh@odnoklassniki.ru", "United Kingdom")]
    expected_df = spark.createDataFrame(expected_data, ["id", "first_name", "last_name", "email", "country"])

    # Check if dataframes are equal
    assert_df_equality(result, expected_df)

def test_clean(spark):
    # Create test data
    client_data = [(1, "Feliza", "Eusden", "feusden0@ameblo.jp", "France"),
                   (8, "Derk", "Mattielli", "dmattielli7@slideshar.net", "United States"),
                   (18, "Richard", "Drinan", "rdrinanh@odnoklassniki.ru", "United Kingdom")]

    client_df = spark.createDataFrame(client_data, ["id", "first_name", "last_name", "email", "country"])

    # Define test configuration
    config_dict = {
        "input_dataset_one_config": {
            "drop_columns": ["first_name", "last_name"],
            "filter_column": "country",
            "filter_criteria": ["United Kingdom", "Netherlands"]
        }
    }

    # Create instance of KommatiParaDataPrepApp
    app = KommatiParaDataPrepApp(config_dict)

    # Define test parameters
    drop_columns = config_dict['input_dataset_one_config']['drop_columns']
    filter_column = config_dict['input_dataset_one_config']['filter_column']
    filter_criteria = config_dict['input_dataset_one_config']['filter_criteria']

    # Call clean method
    result = app.clean(client_df, filter_column, filter_criteria, drop_columns)

    # Define expected result
    expected_data = [(18, "rdrinanh@odnoklassniki.ru", "United Kingdom")]
    expected_df = spark.createDataFrame(expected_data, ["id", "email", "country"])

    # Check if dataframes are equal
    assert_df_equality(result, expected_df)

