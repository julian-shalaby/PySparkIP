from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType


from PySparkIP.src.PySparkIP.PySparkIP import *

spark = SparkSession.builder.appName("PySpark IPAddress").getOrCreate()
SparkIPInit(spark)
schema = StructType([StructField("IPAddress", IPAddressUDT())])

ipDF = spark.read.json("ipMixedFile.json", schema=schema)
ipDF.createOrReplaceTempView("IPAddresses")

class TestUDFs:
    def test_multicast(request):
        # Multicast
        print("Multicast")
        spark.sql("SELECT * FROM IPAddresses WHERE isMulticast(IPAddress)").show()
        ipDF.select('*').filter("isMulticast(IPAddress)").show()
        ipDF.select('*').filter(isMulticast("IPAddress")).show()

    def test_private(request):
        # Private
        print("Private")
        spark.sql("SELECT * FROM IPAddresses WHERE isPrivate(IPAddress)").show()
        ipDF.select('*').filter("isPrivate(IPAddress)").show()
        ipDF.select('*').filter(isPrivate("IPAddress")).show()

    def test_teredo(request):
        # isTeredo
        print("isTeredo")
        spark.sql("SELECT * FROM IPAddresses WHERE isTeredo(IPAddress)").show()

    def test_compressed(request):
        # Compressed
        print("Compressed")
        spark.sql("SELECT compressedIP(IPAddress) FROM IPAddresses").show()

    def test_exploded(request):
        # Exploded
        print("Exploded")
        spark.sql("SELECT explodedIP(IPAddress) FROM IPAddresses").show()

    def test_sorting(request):
        # Sorting
        print("Sorting")
        spark.sql("SELECT * FROM IPAddresses SORT BY ipAsBinary(IPAddress)").show()

    def test_networkContains(request):
        # Network contains
        print("Network contains")
        spark.sql("SELECT * FROM IPAddresses WHERE networkContains(IPAddress, '195.0.0.0/16')").show()
        ipDF.select('*').filter(networkContains('195.0.0.0/16')("IPAddress")).show()

    def test_isIPv4(request):
        # IPv4 check
        print("IPv4 check")
        spark.sql("SELECT * FROM IPAddresses WHERE isIPv4(IPAddress)").show()

    def test_isIPv6(request):
        # IPv6 check
        print("IPv6 check")
        spark.sql("SELECT * FROM IPAddresses WHERE isIPv6(IPAddress)").show()

    def test_setContains(request):
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

        test3 = IPSet({"192.0.0.0", ip2, '10.1.128.0/17', "::", "8::9", '8.7.9.7', '192.0.2.0/26'},
                      spark.sql("SELECT * FROM IPAddresses WHERE isMulticast(IPAddress)").limit(8))
        test3.add('192.0.2.0/26')
        SparkIPSets.add(test3, 'test3')

        ipDF.select('*').filter("setContains(IPAddress, 'test3')").show()
        test2.add(ipDF.select('*').filter(setContains(test3)("IPAddress")))
        test2.remove(ipDF.select('*'))

        test3.showAll()
