from pyspark.sql.types import UserDefinedType, StructField, \
    StructType, StringType, LongType
import ipaddress
from pyspark.sql import SparkSession


class IPAddressUDT(UserDefinedType):
    """
    User-defined type (UDT) for IPAddress.
    """

    @classmethod
    # Stores as string
    def sqlType(cls):
        return LongType()

    @classmethod
    def module(cls):
        return '__main__'

    # Saves num
    def serialize(self, obj):
        return obj.addrNum

    # String to num
    def deserialize(self, datum):
        return IPAddress(datum[0])

    @staticmethod
    def foo():
        pass

    @property
    def props(self):
        return {}


class IPAddress:
    """
    An example class to demonstrate UDT in only Python
    """
    __UDT__ = IPAddressUDT()  # type: ignore

    def __init__(self, addr):
        self.addr = addr
        self.addrNum = int(ipaddress.ip_address(addr))

    def __repr__(self):
        return f"IPAddress({self.addr})"

    def __str__(self):
        return f"{self.addr}"

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
               other.addrNum == self.addrNum


spark = SparkSession.builder.appName("PySpark IPAddress").getOrCreate()

schema = StructType([StructField("IPAddress", IPAddressUDT())])

ipDF = spark.read.json("/Users/julianshalaby/Desktop/PySparkIP/test/ipFile.json", schema=schema)
ipDF.createOrReplaceTempView("IPAddresses")

ipDF.select('*').filter('IPAddress > 123456712345').show()

