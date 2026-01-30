from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.functions import *

class BronzePush:
    @staticmethod
    def run(spark, df: DataFrame) -> DataFrame:
        
        df2 = df.withColumn("DateTimeSend", f.date_trunc("second", f.col("DateTimeSend")))
        df2 = df2.withColumn("OpenDate", f.date_trunc("second", f.col("OpenDate")))

        return df2