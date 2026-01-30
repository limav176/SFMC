from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.functions import *

class BronzeUnsubscribe:
    @staticmethod
    def run(spark, df: DataFrame) -> DataFrame:
        
        df.write.mode("overwrite").option("header",True).option("quote", "\u0000").option("sep","|").csv("/tmp/sparkoutput/auxfile/unsubscribe.csv")
        df2 = spark.read.option("delimiter", ",").option("quote", "\"").option("escape", "\"").option("header", True).csv("/tmp/sparkoutput/auxfile/unsubscribe.csv")

        df2 = df2.withColumn('EventDate', f.date_trunc("second", f.col('EventDate')))
        
        df2 = df2.withColumn('id_email', f.concat(
            f.col("SubscriberKey"),
            f.col("JobId"),
            f.col("BatchId")
            ))
        
        df2 = df2.withColumn('id_unsubscribe', f.concat(
            f.col("Subscriberkey"),
            f.col("JobId"),
            f.col("BatchId"),
            f.col("EventDate")
            ))
        
        df2.createOrReplaceTempView("scr")

        sql="""
            select 
                accountid
                ,id_email
                ,id_unsubscribe
                ,oybaccountid
                ,jobid
                ,listid
                ,batchid
                ,subscriberid
                ,subscriberkey
                ,eventdate
                ,isunique
                ,domain
            from scr
            """
        
        df3=spark.sql(sql)

        return df3
    
