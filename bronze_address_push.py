from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.functions import *

class BronzeAddressPush:
    @staticmethod
    def run(spark, df: DataFrame) -> DataFrame:

        df2 = df.withColumn("CreatedDate", f.date_trunc("second", f.col("CreatedDate")))
        df2 = df2.withColumn("ModifiedDate", f.date_trunc("second", f.col("ModifiedDate")))

        return df2