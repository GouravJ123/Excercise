from pipelines.jobs import kommitpara
from pyspark.sql import SparkSession
from pipelines.utils import configmanagement as cm
from pipelines.utils import logging

def main():
    spark = SparkSession.builder.getOrCreate()

    log = logging.Log4j(spark)

    config = cm.getConfig()

    kommitpara.etl(
        config["path_dataset1"],
        config["path_dataset2"],
        config["country"])

    log.warn("ETL Job FInished successfully")

if __name__ == "__main__":
    main()