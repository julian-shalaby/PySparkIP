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

    def is_ipv4_mapped(self):
        if self.ipaddr.version == 4:
            return False
        if self.ipaddr.ipv4_mapped is not None:
            return True
        return False

    def is_6to4(self):
        if self.ipaddr.version == 4:
            return False
        if self.ipaddr.sixtofour is not None:
            return True
        return False

    def is_teredo(self):
        if self.ipaddr.version == 4:
            return False
        if self.ipaddr.teredo is not None:
            return True
        return False

    def teredo(self):
        if self.ipaddr.version == 4:
            return None
        return self.ipaddr.teredo

    def ipv4_mapped(self):
        if self.ipaddr.version == 4:
            return None
        return self.ipaddr.ipv4_mapped

    def sixtofour(self):
        if self.ipaddr.version == 4:
            return None
        return self.ipaddr.sixtofour
