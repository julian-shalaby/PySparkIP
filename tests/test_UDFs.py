from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType
from src.PySparkIP.PySparkIP import *

spark = SparkSession.builder.appName("PySpark IPAddress").getOrCreate()
PySparkIP(spark)
schema = StructType([StructField("IPAddress", IPAddressUDT())])

ipDF = spark.read.json("ipMixedFile.json", schema=schema)
ipDF.createOrReplaceTempView("IPAddresses")

class TestUDFs:
    def test_bad(self):
        ipDF.select('*').limit(1).show()

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

        net1 = ipaddress.ip_network('::/10')
        ipDF.select('*').filter(networkContains(net1)("IPAddress")).show()

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
        badIPs = IPSet(privateIPs, multicastIPs, '192.0.0.0/16', '::5')
        PySparkIPSets.add(badIPs, 'badIPs')

        checkLog = IPSet(spark.sql("SELECT IPAddress FROM IPAddresses WHERE setContains(IPAddress, 'badIPs')"))

        checkLog.showAll()

    def test_fun(self):
        flaggedIPs = IPSet('192.0.0.0', '::5', '8.0.0.0/24')
        suspiciousIPs = IPSet("10.1.128.9", '0.99.89.83', '244.99.105.29', 'ff67:4d3d:97c4:5203:e903:a1ea:62a7:f99b',
                              '7:8:8::', '88::6', '6.9.0.7', '0.58.18.162', teredoIPs, ipv4ipv6TranslatedIPs)
        incomingLog = IPSet(spark.sql("SELECT IPAddress FROM IPAddresses"))
        incomingLog.remove(spark.sql("SELECT IPAddress FROM IPAddresses"))
        incomingLog.add(spark.sql("SELECT IPAddress FROM IPAddresses"))

        flaggedIPs.add(suspiciousIPs.intersection(incomingLog))
        flaggedIPs.showAll()
