from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.functions import *

class BronzeJourney:
    @staticmethod
    def run(spark, df: DataFrame) -> DataFrame:
        
        df.write.mode("overwrite").option("header",True).option("quote", "\u0000").option("sep","|").csv("/tmp/sparkoutput/auxfile/journey.csv")
        df2 = spark.read.option("delimiter", ",").option("quote", "\"").option("escape", "\"").option("header", True).csv("/tmp/sparkoutput/auxfile/journey.csv")
        df2 = df2.withColumn('ModifiedDate', f.date_trunc("second", f.col('ModifiedDate')))
        df2 = df2.withColumn('CreatedDate', f.date_trunc("second", f.col('CreatedDate')))

        df2.createOrReplaceTempView("scr")

        sql="""
            select 
                versionid
                ,activityid
                ,activityname
                ,activityexternalkey
                ,journeyactivityobjectid
                ,activitytype
                ,journeyid
                ,journeyname
                ,versionnumber
                ,createddate
                ,lastpublisheddate
                ,modifieddate
                ,journeystatus
            from scr
            """
        
        df3=spark.sql(sql)

        return df3
    
