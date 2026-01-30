from pyspark.sql import DataFrame
from pyspark.sql import functions as f

class BronzeSent:
    @staticmethod
    def run(spark, df: DataFrame) -> DataFrame:

        df = df.withColumn('partnerproperties', f.concat(f.lit("['{'Name': 'ListID'',''Value': '"),
            f.col("ListID"),
            f.lit("'}','{'Name': 'SubscriberID'', ''Value': '"),
            f.col("SubscriberID"),
            f.lit("'}']")))
        df = df.withColumn('eventdate', f.date_trunc("second", f.col("eventdate")))
        
        return df