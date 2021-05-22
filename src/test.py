from SparkIP.SparkIP import *
from pyspark.sql.types import StructField, StructType
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PySpark IPAddress").getOrCreate()
SparkIPInit(spark)
schema = StructType([StructField("IPAddress", IPAddressUDT())])

ipDF = spark.read.json("/Users/julianshalaby/Desktop/PySparkIP/tests/ipMixedFile.json", schema=schema)
ipDF.createOrReplaceTempView("IPAddresses")

# Multicast
# print("Multicast")
# spark.sql("SELECT * FROM IPAddresses WHERE isMulticast(IPAddress)").show()
# ipDF.select('*').filter("isMulticast(IPAddress)").show()
# ipDF.select('*').withColumn("col2", isMulticast("IPAddress")).show()
#
ipSet = IPSet("30.0.0.0/8", "::/16", '2001::', '225.0.0.0/16')
SparkIPSets.add(ipSet, 'ipSet')
spark.sql("SELECT * FROM IPAddresses WHERE setContains(IPAddress, 'ipSet')")
ipDF.select('*').filter("setContains(IPAddress, 'ipSet')")
ipDF.select('*').withColumn("setCol", setContains(ipSet)("IPAddress"))
