from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.functions import *

class BronzeMetadadosSfi:
    @staticmethod
    def run(spark, df: DataFrame) -> DataFrame:
        
    
        df.createOrReplaceTempView("scr")

        sql="""
            select 
                dag_name
                ,size as volume
                ,timestamp as sendtime
            from scr
            """
        
        df2=spark.sql(sql)

        return df2
    
