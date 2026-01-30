from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.functions import *

class BronzeSms:
    @staticmethod
    def run(spark, df: DataFrame) -> DataFrame:
        
        df = df.withColumn('CreateDateTime', f.date_trunc("second", f.col('CreateDateTime')))
        df = df.withColumn('ModifiedDateTime', f.date_trunc("second", f.col('ModifiedDateTime')))
        df = df.withColumn('ActionDateTime', f.date_trunc("second", f.col('ActionDateTime')))

        return df