from pyspark.sql.types import StructField, StructType
from pyspark.sql import SparkSession
from SparkIP.SparkIP import *

spark = SparkSession.builder.appName("PySpark IPAddress").getOrCreate()
SparkIPInit(spark)
schema = StructType([StructField("IPAddress", IPAddressUDT())])

ipDF = spark.read.json("ipMixedFile.json", schema=schema)
ipDF.createOrReplaceTempView("IPAddresses")

# Multicast
print("Multicast")
spark.sql("SELECT * FROM IPAddresses WHERE isMulticast(IPAddress)").show()
ipDF.select('*').filter("isMulticast(IPAddress)").show()

# Private
print("Private")
spark.sql("SELECT * FROM IPAddresses WHERE isPrivate(IPAddress)").show()

# isTeredo
print("isTeredo")
spark.sql("SELECT * FROM IPAddresses WHERE isTeredo(IPAddress)").show()

# Compressed
print("Compressed")
spark.sql("SELECT compressedIP(IPAddress) FROM IPAddresses").show()

# Exploded
print("Exploded")
spark.sql("SELECT explodedIP(IPAddress) FROM IPAddresses").show()

# Sorting
print("Sorting")
spark.sql("SELECT * FROM IPAddresses SORT BY ipAsBinary(IPAddress)").show()

# Network contains
print("Network contains")
spark.sql("SELECT * FROM IPAddresses WHERE networkContains(IPAddress, '195.0.0.0/16')").show()

# IPv4 check
print("IPv4 check")
spark.sql("SELECT * FROM IPAddresses WHERE isIPv4(IPAddress)").show()

# IPv6 check
print("IPv6 check")
spark.sql("SELECT * FROM IPAddresses WHERE isIPv6(IPAddress)").show()

# Set contains
print("Set Contains")
ipAddr = ipaddress.ip_address("189.118.188.64")
ip2 = ipaddress.ip_address('41.162.245.45')
ips = {"192.0.0.0", "5422:6622:1dc6:366a:e728:84d4:257e:655a", ip2, "::"}
test = IPSet(ipAddr, ips)
test2 = IPSet('192.0.2.0/26', '192.168.8.128/25', '10.0.1.0/24', "::", "8::9", '10.1.128.0/17')
SparkIPSets.add(test, 'test')
SparkIPSets.add(test2, 'test2')
spark.sql("SELECT * FROM IPAddresses WHERE setContains(IPAddress, 'test')").show()

# Set contains on another set
print("Set contains on another set")
spark.sql("SELECT * FROM IPAddresses WHERE setContains(IPAddress, 'test2')").show()

test3 = IPSet({"192.0.0.0", ip2, '10.1.128.0/17', "::", "8::9", '8.7.9.7', '192.0.2.0/26'})
test3.add('192.0.2.0/26')

