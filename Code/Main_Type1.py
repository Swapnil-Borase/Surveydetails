from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *


if __name__ == '__main__':
    spark=SparkSession.builder.appName("Survey_details").master("local[*]").getOrCreate()
    print(spark)

    Survey_Schema=StructType([StructField("Year", IntegerType()),
                              StructField("Industry_aggregation",StringType()),
                              StructField("Industry_code",StringType()),
                              StructField("Industry_name",StringType()),
                              StructField("Units",StringType()),
                              StructField("Variable_code",StringType()),
                              StructField("Variable_name",StringType()),
                              StructField("Variable_category",StringType()),
                              StructField("Value", LongType()),
                              StructField("Industry_code_ANZ",StringType())])

    ## Creating Dataframe

    Survey_df=spark.read.csv(path=r"E:\Data Cloud Engineer\Pyspark\Pysparkinput files\Survey Details\annual-enterprise-survey-financial-year-provisional_2022_05_21.csv",inferSchema=True,header=True)
    Survey_df.show()
    Survey_df.printSchema()

    Survey_df1=Survey_df.withColumn("Date",to_date(lit("2022-05-21")))
    Survey_df1.show()
    Survey_df1.write.option("header", True).csv(r"E:\Data Cloud Engineer\Sparkproject\Surveydetails\Target")
