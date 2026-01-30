from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.functions import *

class BronzeBounce:
    @staticmethod
    def run(spark, df: DataFrame) -> DataFrame:
        
        df2 = df.withColumn("EventDate", f.date_trunc("second", f.col("EventDate")))
        
        df2 = df2.withColumn('id_email', f.concat(
            f.col("SubscriberKey"),
            f.col("JobId"),
            f.col("BatchId")
            ))
        
        df2 = df2.withColumn('id_bounce', f.concat(
            f.col("SubscriberKey"),
            f.col("JobId"),
            f.col("BatchId"),
            f.col("EventDate")
            ))

        return df2
    
