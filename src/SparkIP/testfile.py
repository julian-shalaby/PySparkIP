from pyspark.sql.types import UserDefinedType, StructField, \
    StructType, StringType, LongType
import ipaddress
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from IPAddressUDT import *


spark = SparkSession.builder.appName("PySpark IPAddress").getOrCreate()

schema = StructType([StructField("IPAddress", IPAddressUDT())])

ipDF = spark.read.json("/Users/julianshalaby/Desktop/PySparkIP/test/ipMixedFile.json", schema=schema)
ipDF.createOrReplaceTempView("IPAddresses")

spark.udf.register("isMulticast", lambda ip: ip.is_multicast(), "boolean")

ipDF.select('*').filter("isMulticast(IPAddress)").show()
spark.sql("SELECT * FROM IPAddresses WHERE isMulticast(IPAddress)").show()
