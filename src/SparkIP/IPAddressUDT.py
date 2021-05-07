from pyspark.sql.types import UserDefinedType, StringType
import ipaddress


class IPAddressUDT(UserDefinedType):
    """
    User-defined type (UDT) for IPAddress.
    """

    @classmethod
    def sqlType(cls):
        return StringType()

    @classmethod
    def module(cls):
        return '__main__'

    def serialize(self, obj):
        return obj.addr

    def deserialize(self, datum):
        return IPAddress(datum)


class IPAddress:
    __UDT__ = IPAddressUDT()

    def __init__(self, addr):
        self.addr = addr
        self.ipaddr = ipaddress.ip_address(addr)

    def is_multicast(self):
        if self.ipaddr.is_multicast:
            return True
        else:
            return False
