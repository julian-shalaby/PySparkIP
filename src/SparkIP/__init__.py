from pyspark.sql.types import UserDefinedType, StructField, \
    StructType, StringType
import ipaddress


class IPAddrUDT(UserDefinedType):
    """
    SQL user-defined type (UDT) for an IP address.
    """

    @classmethod
    def sqlType(cls):
        return StructType([
            StructField("addrInternal", StringType(), False)
        ])

    # @classmethod
    # def module(cls):
    #     return "pyspark.mllib.linalg"

    # @classmethod
    # def scalaUDT(cls):
    #     return "org.apache.spark.mllib.linalg.VectorUDT"

    def serialize(self, obj):
        if isinstance(obj, IPAddr):
            addr_str = str(obj.addrInternal)
            return (addr_str,)
        else:
            raise TypeError("cannot serialize %r of type %r" %
                            (obj, type(obj)))

    def deserialize(self, datum):
        assert len(datum) == 1, \
            "IPAddrUDT.deserialize given row with length %d but requires 1" % \
            len(datum)
        addrStr = datum[0]
        return IPAddr(addrStr)

    def simpleString(self):
        return "ipaddr"


class IPAddr(object):

    __UDT__ = IPAddrUDT()

    def __init__(self, str_):
        self.addrInternal = ipaddress.ip_address(str_)

    def __str__(self):
        return str(self.addrInternal)

    def __repr__(self):
        return repr(self.addrInternal)

    def __eq__(self, other):
        if isinstance(other, IPAddr):
            return other.addrInternal == self.addrInternal
        return False

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self.addrInternal)

    def __reduce__(self):
        return IPAddr, (str(self.addrInternal),)


__version__ = "1.0.2"
