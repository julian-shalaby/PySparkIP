import unittest
from SparkIP.SparkIP import *
import ipaddress
from pyspark.sql.types import StructField, StructType
from pyspark.sql.functions import col
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("PySpark IPAddress").getOrCreate()
SparkIPInit(spark)
schema = StructType([StructField("IPAddress", IPAddressUDT())])

ipDF = spark.read.json("/Users/julianshalaby/Desktop/PySparkIP/tests/ipMixedFile.json", schema=schema)
ipDF.createOrReplaceTempView("IPAddresses")

class IPSetTest(unittest.TestCase):
    def test_returnAll(self):
        net1 = ipaddress.ip_network("::/17")
        net2 = ipaddress.ip_network("0.0.0.0/16")
        net3 = ipaddress.ip_network("192.0.0.0/16")
        net4 = ipaddress.ip_network("225.0.0.0/8")
        net5 = ipaddress.ip_network("1::/16")
        net6 = ipaddress.ip_network("1::/16")
        net7 = ipaddress.ip_network("111.0.0.0/8")
        net8 = ipaddress.ip_network("121.0.0.0/16")

        ip1 = "192.0.0.0"
        ip2 = '0.0.0.0'
        ip3 = '::'
        ip4 = "2001::"

        set1 = IPSet(ip1, ip2, ip3, ip4, net1, net2, net3, net4, net5, net6, net7, net8)
        set2 = IPSet(ip1, ip2, ip3, ip4, net8, net1, net3, net2, net5, net6, net4, net7)

        self.assertEqual(set1.returnAll(), set2.returnAll())

    def test_Intersects(self):
        net1 = ipaddress.ip_network("::/17")
        net2 = ipaddress.ip_network("0.0.0.0/16")
        net3 = ipaddress.ip_network("192.0.0.0/16")
        net4 = ipaddress.ip_network("225.0.0.0/8")
        net5 = ipaddress.ip_network("1::/16")
        net6 = ipaddress.ip_network("1::/16")
        net7 = ipaddress.ip_network("111.0.0.0/8")
        net8 = ipaddress.ip_network("121.0.0.0/16")

        ip1 = "192.0.0.0"
        ip2 = '0.0.0.0'
        ip3 = '::'
        ip4 = "2001::"

        set1 = IPSet(net1, net2, net3, net5, net6, net8, ip2, ip3)
        set2 = IPSet(net1, net4, net3, net5, net7, net8, ip1, ip3, ip2, ip4)

        self.assertEqual(set1.intersects(set2), IPSet(net1, net3, net5, net8, ip2, ip3))


if __name__ == '__main__':
    unittest.main()
