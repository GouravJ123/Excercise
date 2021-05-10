# Example ETL with no parameters - see etl() function

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, current_timestamp
from pipelines.utils import transformations, configmanagement as cm
from pipelines.utils import logging

spark = SparkSession.builder.getOrCreate()

def extract_data(filePath):
    """ Extracting the datasets """
    return spark.read.format("csv").options(header = 'true', delimiter = ',').load(filePath)

def transform_data(df_ds1, df_ds2, country):
    """Transform original dataset.

    :param df: Input DataFrame.
    :return: Transformed DataFrame.
    """
    
    df_ds1_select = df_ds1.select("id","email","country")
    df_ds1_renamed = transformations.renameColumn(
        df_ds1_select,
        "id",
        "client_identifier")
    
    df_ds1_transformed = transformations.filterdf(
        df_ds1_renamed,
        col("country"),
        country)
    
    df_ds2_drop = df_ds2.drop("cc_n")
    df_renamed_id = transformations.renameColumn(
        df_ds2_drop,
        "id",
        "client_identifier"
    )

    df_renamed_btc_a = transformations.renameColumn(
        df_renamed_id,
        "btc_a",
        "bitcoin_address"
    )
    
    df_ds2_transformed = transformations.renameColumn(
        df_renamed_btc_a,
        "cc_t",
        "credit_card_type"
    )
                    
    df_transformed = df_ds1_transformed.join(
        df_ds2_transformed,
        df_ds1_transformed.client_identifier == df_ds2_transformed.client_identifier,
        "inner").drop(
            df_ds2_transformed.client_identifier)
    
    return df_transformed

def load_data(df):
    """Collect data locally and write to CSV.

    :param df: DataFrame to print.
    :return: None
    """
    (df
     .write
     .csv('client_data', mode='overwrite', header=True))
    return None

def etl(dataset1_location, dataset2_location, country):
    df_ds1 = extract_data(dataset1_location)
    df_ds2 = extract_data(dataset2_location)
    df_transformed = transform_data(df_ds1, df_ds2, country)
    load_data(df_transformed)