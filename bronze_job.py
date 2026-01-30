from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.functions import *

class BronzeJob:
    @staticmethod
    def run(spark, df: DataFrame) -> DataFrame:
        
        df2 = df.withColumn('ModifiedDate', f.date_trunc("second", f.col('ModifiedDate')))

        df2.createOrReplaceTempView("scr")

        sql="""
            select 
                jobid
                ,emailid
                ,accountid
                ,accountuserid
                ,fromname
                ,fromemail
                ,schedtime
                ,pickuptime
                ,deliveredtime
                ,eventid
                ,ismultipart
                ,jobtype
                ,jobstatus
                ,modifiedby
                ,modifieddate
                ,emailname
                ,emailsubject
                ,iswrapped
                ,testemailaddr
                ,category
                ,bccemail
                ,originalschedtime
                ,createddate
                ,characterset
                ,ipaddress
                ,salesforcetotalsubscribercount
                ,salesforceerrorsubscribercount
                ,sendtype
                ,dynamicemailsubject
                ,suppresstracking
                ,sendclassificationtype
                ,sendclassification
                ,resolvelinkswithcurrentdata
                ,emailsenddefinition
                ,deduplicatebyemail
                ,triggerersenddefinitionobjectid
                ,triggeredsendcustomerkey
            from scr
            """
        
        df3=spark.sql(sql)

        return df3
    
