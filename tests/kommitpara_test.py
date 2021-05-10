from chispa.dataframe_comparer import *
from context import pipelines
from pipelines.jobs import kommitpara
from pipelines.utils import logging
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
log = logging.Log4j(spark)

log.warn("Testing started")    
def test_transform():

    source_dataset1 = [
        (105,"Lacie","Infante","linfante2w@telegraph.co.uk","Netherlands")
    ]
    
    schema_dataset1 = ["id","first_name","last_name","email","country"]
    
    df_source_dataset1 = spark.createDataFrame(data=source_dataset1, 
                                               schema = schema_dataset1)
    
    source_dataset2 = [
        (105,"13j6FKzrLgumLUqeYH4baeY5qZgiwGW5UC","jcb","3538483164066576")
    ]
    
    schema_dataset2 = ["id","btc_a","cc_t","cc_n"]
    
    df_source_dataset2 = spark.createDataFrame(data=source_dataset2, 
                                               schema = schema_dataset2)
    
    expected_dataset = [
        (105,"linfante2w@telegraph.co.uk","Netherlands","13j6FKzrLgumLUqeYH4baeY5qZgiwGW5UC","jcb")
        ]
    
    schema_expected_dataset = ["client_identifier","email","country","bitcoin_address","credit_card_type"]
    
    df_expected = spark.createDataFrame(data=expected_dataset, 
                                               schema = schema_expected_dataset)
    
    df_actual = kommitpara.transform_data(df_source_dataset1, df_source_dataset2, "Netherlands")
    
    assert_df_equality(df_actual, df_expected)
    
test_transform()

print("Testing completed")