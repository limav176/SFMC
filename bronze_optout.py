from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.functions import *

class BronzeOptout:
    @staticmethod
    def run(spark, df: DataFrame) -> DataFrame:
        
        df2 = df.withColumn('DateUnsubscribed', f.date_trunc("second", f.col('DateUnsubscribed')))

        df2.createOrReplaceTempView("scr")

        sql="""
            select 
                SubscriberType
                ,DateUnsubscribed
                ,status
                ,DateJoined
                ,subscriberkey
            from scr
            """
        
        df3=spark.sql(sql)

        return df3
    
