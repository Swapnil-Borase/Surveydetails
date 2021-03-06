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

    Survey_df=spark.read.csv(path=r"E:\DataCloudEngineer\Pyspark\Pysparkinputfiles\Survey Details\annual-enterprise-survey-financial-year-provisional_2022_05_21.csv",inferSchema=True,header=True)
    Survey_df.show()
    Survey_df.printSchema()

    Survey_df1=Survey_df.withColumn("From_Date",to_date(lit("2022-05-21"))).withColumn("To_Date",to_date(lit("2099-06-12")))
    Survey_df1.show()


    ## to save in target
    Survey_df1.write.option("header",True).mode('overwrite').csv(r"E:\DataCloudEngineer\Sparkproject\Surveydetails\Target\type_2")

