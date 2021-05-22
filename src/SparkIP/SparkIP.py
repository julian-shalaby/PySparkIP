from .IPAddressUDT import *
from .AVL_Tree import *
import warnings
from pyspark.sql.types import BooleanType, LongType, StringType
from pyspark.sql.functions import udf


class IPSet:
    def __init__(self, *ips):
        # Hash map for IP addresses, AVL Tree for IP networks
        self.ipMap = {}
        self.netAVL = AVL_Tree()
        self.root = None

        # Iterate through all IPs passed through
        for ip in ips:
            # If its an IP UDT, extract the UDTs value, add it to the map, then go to the next iteration
            if isinstance(ip, IPAddress):
                self.ipMap[str(ip.ipaddr)] = int(ip.ipaddr)
                continue

            # If its an IPSet, extract all of its values to a list and add it to the set
            if isinstance(ip, IPSet):
                ip = ip.returnAll()

            # If its a list, tuple, or set, iterate through it and add each element to the set
            if type(ip) is list or type(ip) is tuple or type(ip) is set:
                for element in ip:
                    # Try converting each element to an ipaddress object.
                    # If it succeeds, add it to the IP address map
                    try:
                        ipaddr = ipaddress.ip_address(element)
                        self.ipMap[str(ipaddr)] = int(ipaddr)
                    # If it fails, it is a network. Add it to the IP network tree
                    except:
                        self.root = self.netAVL.insert(self.root, ipaddress.ip_network(element))
                # Continue to the next input after iterating through the list
                continue

            # If its not a list, tuple, set, or UDT, try converting it to an ipaddress object.
            # If it succeeds, add it to the IP address map
            try:
                ipaddr = ipaddress.ip_address(ip)
                self.ipMap[str(ipaddr)] = int(ipaddr)
            # If it fails, it is a network. Add it to the IP network tree
            except:
                self.root = self.netAVL.insert(self.root, ipaddress.ip_network(ip))

    def add(self, *ips):
        # Iterate through all IPs passed through
        for ip in ips:
            # If its an IP UDT, extract the UDTs value, add it to the map, then go to the next iteration
            if isinstance(ip, IPAddress):
                self.ipMap[str(ip.ipaddr)] = int(ip.ipaddr)
                continue

            # If its an IPSet, extract all of its values to a list and add it to the set
            if isinstance(ip, IPSet):
                ip = ip.returnAll()

            # If its a list, tuple, or set, iterate through it and add each element to the set
            if type(ip) is list or type(ip) is tuple or type(ip) is set:
                # Try converting each element to an ipaddress object.
                # If it succeeds, add it to the IP address map
                for element in ip:
                    try:
                        ipaddr = ipaddress.ip_address(element)
                        self.ipMap[str(ipaddr)] = int(ipaddr)
                    # If it fails, it is a network. Add it to the IP network tree
                    except:
                        self.root = self.netAVL.insert(self.root, ipaddress.ip_network(element))
                # Continue to the next input after iterating through the list
                continue

            # If its not a list, tuple, set, or UDT, try converting it to an ipaddress object.
            # If it succeeds, add it to the IP address map
            try:
                ipaddr = ipaddress.ip_address(ip)
                self.ipMap[str(ipaddr)] = int(ipaddr)
            # If it fails, it is a network. Add it to the IP network tree
            except:
                self.root = self.netAVL.insert(self.root, ipaddress.ip_network(ip))

        # Re-register the IPSet UDF or else this set won't be updated in the UDF
        update_sets()

    def remove(self, *ips):
        # Iterate through all IPs passed through
        for ip in ips:
            # If its an IP UDT, extract the UDTs value, remove it to from map, then go to the next iteration
            if isinstance(ip, IPAddress):
                del self.ipMap[str(ip.ipaddr)]
                continue

            # If its a list, tuple, or set, iterate through it and remove each element from the set
            if type(ip) is list or type(ip) is tuple or type(ip) is set:
                # Try converting each element to an ipaddress object.
                # If it succeeds, remove it from the IP address map
                for element in ip:
                    try:
                        del self.ipMap[str(ipaddress.ip_address(element))]
                    # If it fails, it is a network. Remove it from the IP network tree
                    except:
                        self.root = self.netAVL.delete(self.root, ipaddress.ip_network(element))
                # Continue to the next input after iterating through the list
                continue

            # If its not a list, tuple, set, or UDT, try converting it to an ipaddress object.
            # If it succeeds, remove it from the IP address map
            try:
                del self.ipMap[str(ipaddress.ip_address(ip))]
            except:
                # If it fails, it is a network. Remove it from the IP network tree
                self.root = self.netAVL.delete(self.root, ipaddress.ip_network(ip))

        # Re-register the IPSet UDF or else this set won't be updated in the UDF
        update_sets()

    def contains(self, ip):
        if isinstance(ip, IPAddress):
            # Check if its in the IP address hash map
            if str(ip.ipaddr) in self.ipMap:
                return True
            # Check if its in the IP network tree
            if AVL_Tree.avl_search_ipAddr(self.root, ip.ipaddr) is True:
                return True
            return False
        else:
            # If its not a UDT, try converting it to an ipaddress object.
            # If it succeeds, check if its in the set.
            try:
                ipAddr = ipaddress.ip_address(ip)
                if str(ipAddr) in self.ipMap:
                    return True
                if AVL_Tree.avl_search_ipAddr(self.root, ipAddr) is True:
                    return True
            # If it fails to convert, it is a network. Check if its in the IP network tree
            except:
                # If found, continue to next iter. If not found, return false
                if AVL_Tree.avl_search(self.root, ipaddress.ip_network(ip)) is True:
                    return True
            return False

    # Remove every item from the set
    def clear(self):
        self.ipMap = {}
        self.root = None
        self.netAVL = AVL_Tree()
        update_sets()

    # Show every address and network in the set
    def showAll(self):
        print('IP addresses:')
        for i in self.ipMap.keys():
            print(i)
        print('IP networks:')
        self.netAVL.preOrder(self.root)

    def returnAll(self):
        set_list = []
        for i in self.ipMap.keys():
            set_list.append(i)
        set_list.extend(self.netAVL.returnAll(self.root))
        return set_list

    # Check if the set is empty
    def isEmpty(self):
        if not self.ipMap and self.root is None:
            return True
        return False

    # Get the set intersection
    def intersects(self, set2):
        intersectSet = IPSet()
        for i in self.ipMap.keys():
            if set2.contains(i):
                intersectSet.add(i)
        intersectSet.add(self.netAVL.netIntersect(self.root, set2))
        return intersectSet

    # Get the union of 2 sets
    def union(self, set2):
        unionSet = IPSet()
        for i in self.ipMap.keys():
            unionSet.add(i)
        for i in set2.ipMap.keys():
            unionSet.add(i)

        unionSet.add(self.netAVL.returnAll(self.root))
        unionSet.add(set2.netAVL.returnAll(set2.root))
        return unionSet

    # Get the diff of 2 sets
    def diff(self, set2):
        diffSet = IPSet()
        for i in self.ipMap.keys():
            if set2.contains(i) is False:
                diffSet.add(i)

        diffSet.add(self.netAVL.returnAll(self.root))
        diffSet.remove(self.netAVL.netIntersect(self.root, set2))

        return diffSet

    def __eq__(self, set2):
        if self.returnAll() == set2.returnAll():
            return True
        return False

    def __ne__(self, set2):
        if self.returnAll() != set2.returnAll():
            return True
        return False


# A hash map from string -> IPSet to use IPSets in UDFs
# Can't pass objects to UDFs, so we pass set names and internally map set names to the actual object
# This contains each IPSet registered for use in the UDFs
class SetMap:
    def __init__(self):
        self.setMap = {}

    # Add a set to the map by passing through its name and the object
    def add(self, set_to_add: IPSet, set_name: str):
        self.setMap[set_name] = set_to_add
        update_sets()

    # Remove sets by passing their names
    def remove(self, *set_name: str):
        for i in set_name:
            del self.setMap[i]
        update_sets()

    # Clear to whole set map
    def clear(self):
        self.setMap = {}
        update_sets()

    # Check what sets are registered
    def setsAvailable(self):
        for x in self.setMap.keys():
            print(x)


# The set map to use with the API
SparkIPSets = SetMap()


# Pass through a spark session variable to register all UDF functions
def SparkIPInit(spark, log_level=None):
    if log_level is None:
        warnings.warn("No log level specified for SparkIP. Setting log level to WARN.", Warning)
        log_level = "WARN"
    """Address Types"""
    # Multicast
    spark.udf.register("isMulticast", lambda ip: ip.ipaddr.is_multicast, "boolean")
    # Private
    spark.udf.register("isPrivate", lambda ip: ip.ipaddr.is_private, "boolean")
    # Global
    spark.udf.register("isGlobal", lambda ip: ip.ipaddr.is_global, "boolean")
    # Unspecified
    spark.udf.register("isUnspecified", lambda ip: ip.ipaddr.is_unspecified, "boolean")
    # Reserved
    spark.udf.register("isReserved", lambda ip: ip.ipaddr.is_reserved, "boolean")
    # Loopback
    spark.udf.register("isLoopback", lambda ip: ip.ipaddr.is_loopback, "boolean")
    # Link local
    spark.udf.register("isLinkLocal", lambda ip: ip.ipaddr.is_link_local, "boolean")
    # Site local
    spark.udf.register("isSiteLocal", lambda ip: ip.ipaddr.is_site_local, "boolean")
    # isIPv4 mapped
    spark.udf.register("isIPv4Mapped", lambda ip: ip.is_ipv4_mapped(), "boolean")
    # is6to4
    spark.udf.register("is6to4", lambda ip: ip.is_6to4(), "boolean")
    # isTeredo
    spark.udf.register("isTeredo", lambda ip: ip.is_teredo(), "boolean")
    # Compressed
    spark.udf.register("compressedIP", lambda ip: ip.ipaddr.compressed, "string")
    # Exploded
    spark.udf.register("explodedIP", lambda ip: ip.ipaddr.exploded, "string")

    """IP as a number"""""
    # spark only supports long correctly. only use on IPv4
    spark.udf.register("ipv4AsNum", lambda ip: int(ip.ipaddr), "long")

    """IP as a binary string"""""
    spark.udf.register("ipAsBinary", lambda ip: format(int(ip.ipaddr), '0128b'), "string")

    """Network functions"""
    # Net contains
    spark.udf.register("networkContains", lambda ip, net: ip.ipaddr in ipaddress.ip_network(net),
                       "boolean")

    """Other functions"""
    # IPv4 check
    spark.udf.register("isIPv4", lambda ip: ip.ipaddr.version == 4, "boolean")
    # IPv6 check
    spark.udf.register("isIPv6", lambda ip: ip.ipaddr.version == 6, "boolean")
    # So IPSet can reuse the spark session variable and reset log level
    update_sets(spark, log_level)


# Each time an IP Set is updated, its function has to re-register to reflect these changes
def update_sets(spark=None, log_level="WARN"):
    """IPSet functions"""
    # Set contains
    update_sets.spark = spark or update_sets.spark
    update_sets.log_level = log_level or update_sets.log_level
    update_sets.spark.sparkContext.setLogLevel("FATAL")
    update_sets.spark.udf.register("setContains", lambda ip, ip_set: SparkIPSets.setMap[ip_set].contains(ip), "boolean")
    update_sets.spark.sparkContext.setLogLevel(log_level)


# Pure UDFs
"""Address Types"""
isMulticast = udf(lambda ip: ip.ipaddr.is_multicast, BooleanType())
isGlobal = udf(lambda ip: ip.ipaddr.is_global, BooleanType())
isPrivate = udf(lambda ip: ip.ipaddr.is_private, BooleanType())
isUnspecified = udf(lambda ip: ip.ipaddr.is_unspecified, BooleanType())
isReserved = udf(lambda ip: ip.ipaddr.is_reserved, BooleanType())
isLoopback = udf(lambda ip: ip.ipaddr.is_loopback, BooleanType())
isLinkLocal = udf(lambda ip: ip.ipaddr.is_link_local, BooleanType())
isSiteLocal = udf(lambda ip: ip.ipaddr.is_site_local, BooleanType())
isIPv4Mapped = udf(lambda ip: ip.is_ipv4_mapped(), BooleanType())
is6to4 = udf(lambda ip: ip.is_6to4(), BooleanType())
isTeredo = udf(lambda ip: ip.is_teredo(), BooleanType())
compressedIP = udf(lambda ip: ip.ipaddr.compressed, StringType())
explodedIP = udf(lambda ip: ip.ipaddr.exploded, StringType())
"""IP as a number"""""
# spark only supports long correctly. only use on IPv4
ipv4AsNum = udf(lambda ip: int(ip.ipaddr), LongType())
"""IP as a binary string"""""
ipAsBinary = udf(lambda ip: format(int(ip.ipaddr), '0128b'), StringType())
"""Network functions"""
def networkContains(ipnet):
    return udf(lambda ip: ip.ipaddr in ipaddress.ip_network(ipnet))
"""Other functions"""
isIPv4 = udf(lambda ip: ip.ipaddr.version == 4, BooleanType())
isIPv6 = udf(lambda ip: ip.ipaddr.version == 6, BooleanType())
"""IPSet functions"""
def setContains(ipset):
    return udf(lambda ip: ipset.contains(ip))


# Other functions (not for SparkSQL use)
def nets_intersect(net1, net2):
    net1 = ipaddress.ip_network(net1)
    net2 = ipaddress.ip_network(net2)
    if net1.network_address <= net2.broadcast_address and net1.broadcast_address >= net2.network_address:
        return True
    return False
