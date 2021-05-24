import pyspark

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

        # Iterate through all IPs passed through
        for ip in ips:
            # If its an IP UDT, extract the UDTs value, add it to the map, then go to the next iteration
            if isinstance(ip, IPAddress):
                self.ipMap[str(ip.ipaddr)] = int(ip.ipaddr)
                continue

            # If its an IPSet, extract all of its values to a list and add it to the set
            if isinstance(ip, IPSet):
                ip = ip.returnAll()

            if isinstance(ip, pyspark.sql.dataframe.DataFrame):
                ip = list(ip.toPandas()[ip.schema.names[0]])
                for i in ip:
                    self.ipMap[str(i.ipaddr)] = int(i.ipaddr)
                continue

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
                        self.netAVL.insert(ipaddress.ip_network(element))
                # Continue to the next input after iterating through the list
                continue

            # If its not a list, tuple, set, or UDT, try converting it to an ipaddress object.
            # If it succeeds, add it to the IP address map
            try:
                ipaddr = ipaddress.ip_address(ip)
                self.ipMap[str(ipaddr)] = int(ipaddr)
            # If it fails, it is a network. Add it to the IP network tree
            except:
                self.netAVL.insert(ipaddress.ip_network(ip))

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

            if isinstance(ip, pyspark.sql.dataframe.DataFrame):
                ip = list(ip.toPandas()[ip.schema.names[0]])
                for i in ip:
                    self.ipMap[str(i.ipaddr)] = int(i.ipaddr)
                continue

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
                        self.netAVL.insert(ipaddress.ip_network(element))
                # Continue to the next input after iterating through the list
                continue

            # If its not a list, tuple, set, or UDT, try converting it to an ipaddress object.
            # If it succeeds, add it to the IP address map
            try:
                ipaddr = ipaddress.ip_address(ip)
                self.ipMap[str(ipaddr)] = int(ipaddr)
            # If it fails, it is a network. Add it to the IP network tree
            except:
                self.netAVL.insert(ipaddress.ip_network(ip))

        # Re-register the IPSet UDF or else this set won't be updated in the UDF
        update_sets()

    def remove(self, *ips):
        # Iterate through all IPs passed through
        for ip in ips:
            # If its an IP UDT, extract the UDTs value, remove it to from map, then go to the next iteration
            if isinstance(ip, IPAddress):
                if str(ip.ipaddr) in self.ipMap:
                    del self.ipMap[str(ip.ipaddr)]
                continue

            # If its an IPSet, extract all of its values to a list and add it to the set
            if isinstance(ip, IPSet):
                ip = ip.returnAll()

            if isinstance(ip, pyspark.sql.dataframe.DataFrame):
                ip = list(ip.toPandas()[ip.schema.names[0]])
                for i in ip:
                    if str(i.ipaddr) in self.ipMap:
                        del self.ipMap[str(i.ipaddr)]
                continue

            # If its a list, tuple, or set, iterate through it and remove each element from the set
            if type(ip) is list or type(ip) is tuple or type(ip) is set:
                # Try converting each element to an ipaddress object.
                for element in ip:
                    try:
                        del self.ipMap[str(ipaddress.ip_address(element))]
                    except:
                        pass
                    try:
                        self.netAVL.delete(ipaddress.ip_network(element))
                    except:
                        pass
                # Continue to the next input after iterating through the list
                continue

            # If its not a list, tuple, set, or UDT, try converting it to an ipaddress object.
            try:
                del self.ipMap[str(ipaddress.ip_address(ip))]
            except:
                pass
            try:
                self.netAVL.delete(ipaddress.ip_network(ip))
            except:
                pass

        # Re-register the IPSet UDF or else this set won't be updated in the UDF
        update_sets()

    def contains(self, ip):
        if isinstance(ip, IPAddress):
            # Check if its in the IP address hash map
            if str(ip.ipaddr) in self.ipMap:
                return True
            # Check if its in the IP network tree
            if self.netAVL.avl_search_ipAddr(ip.ipaddr) is True:
                return True
            return False
        else:
            # If its not a UDT, try converting it to an ipaddress object.
            # If it succeeds, check if its in the set.
            try:
                ipAddr = ipaddress.ip_address(ip)
                if str(ipAddr) in self.ipMap:
                    return True
                if self.netAVL.avl_search_ipAddr(ipAddr) is True:
                    return True
            # If it fails to convert, it is a network. Check if its in the IP network tree
            except:
                # If found, continue to next iter. If not found, return false
                if self.netAVL.avl_search(ipaddress.ip_network(ip)) is True:
                    return True
            return False

    # Remove every item from the set
    def clear(self):
        self.ipMap = {}
        self.netAVL.root = None
        update_sets()

    # Show every address and network in the set
    def showAll(self):
        print('IP addresses:')
        for i in self.ipMap.keys():
            print(i)
        print('IP networks:')
        self.netAVL.preOrder()

    def returnAll(self):
        set_set = set()
        for i in self.ipMap.keys():
            set_set.add(i)
        set_set.update(self.netAVL.returnAll())
        return set_set

    # Check if the set is empty
    def isEmpty(self):
        if not self.ipMap and self.netAVL.root is None:
            return True
        return False

    # Get the set intersection
    def intersection(self, set2):
        intersectSet = IPSet()
        if len(self) <= len(set2):
            for i in self.ipMap.keys():
                if set2.contains(i):
                    intersectSet.add(i)
            intersectSet.add(self.netAVL.netIntersect(set2))
        else:
            for i in set2.ipMap.keys():
                if self.contains(i):
                    intersectSet.add(i)
            intersectSet.add(set2.netAVL.netIntersect(self))
        return intersectSet

    # Get the union of 2 sets
    def union(self, set2):
        return IPSet(self, set2)

    # Get the diff of 2 sets
    def diff(self, set2):
        diffSet = self
        diffSet.remove(diffSet.intersection(set2))
        return diffSet

    def __eq__(self, set2):
        if self.returnAll() == set2.returnAll():
            return True
        return False

    def __ne__(self, set2):
        if self.returnAll() != set2.returnAll():
            return True
        return False

    def __len__(self):
        return len(self.ipMap) + self.netAVL.length


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
            if i in self.setMap:
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

# Pass through a spark session variable to register all UDF functions
def SparkIPInit(spark, log_level=None):
    if log_level is None:
        warnings.warn("No log level specified for SparkIP. Setting log level to WARN.", Warning)
        log_level = "WARN"
    """Address Types"""
    # Multicast
    spark.udf.register("isMulticast", isMulticast)
    # Private
    spark.udf.register("isPrivate", isPrivate)
    # Global
    spark.udf.register("isGlobal", isGlobal)
    # Unspecified
    spark.udf.register("isUnspecified", isUnspecified)
    # Reserved
    spark.udf.register("isReserved", isReserved)
    # Loopback
    spark.udf.register("isLoopback", isLoopback)
    # Link local
    spark.udf.register("isLinkLocal", isLinkLocal)
    # Site local
    spark.udf.register("isSiteLocal", isSiteLocal)
    # isIPv4 mapped
    spark.udf.register("isIPv4Mapped", isIPv4Mapped)
    # is6to4
    spark.udf.register("is6to4", is6to4)
    # isTeredo
    spark.udf.register("isTeredo", isTeredo)
    # Compressed
    spark.udf.register("compressedIP", compressedIP)
    # Exploded
    spark.udf.register("explodedIP", explodedIP)

    """IP as a number"""""
    # spark only supports long correctly. only use on IPv4
    spark.udf.register("ipv4AsNum", ipv4AsNum)

    """IP as a binary string"""""
    spark.udf.register("ipAsBinary", ipAsBinary)

    """Network functions"""
    # Net contains
    spark.udf.register("networkContains", lambda ip, net: ip.ipaddr in ipaddress.ip_network(net),
                       "boolean")

    """Other functions"""
    # IPv4 check
    spark.udf.register("isIPv4", isIPv4)
    # IPv6 check
    spark.udf.register("isIPv6", isIPv6)
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


"""Network functions"""
def networkContains(ipnet):
    return udf(lambda ip: ip.ipaddr in ipaddress.ip_network(ipnet), BooleanType())
"""Other functions"""
isIPv4 = udf(lambda ip: ip.ipaddr.version == 4, BooleanType())
isIPv6 = udf(lambda ip: ip.ipaddr.version == 6, BooleanType())
"""IPSet functions"""
def setContains(ipset):
    return udf(lambda ip: ipset.contains(ip), BooleanType())


# Other functions (not for SparkSQL use)
def netsIntersect(net1, net2):
    net1 = ipaddress.ip_network(net1)
    net2 = ipaddress.ip_network(net2)
    if net1.network_address <= net2.broadcast_address and net1.broadcast_address >= net2.network_address:
        return True
    return False


multicastIPs = {'224.0.0.0/4', 'ff00::/8'}
privateIPs = {'0.0.0.0/8', '10.0.0.0/8', '127.0.0.0/8', '169.254.0.0/16', '172.16.0.0/12', '192.0.0.0/29',
              '192.0.0.170/31', '192.0.2.0/24', '192.168.0.0/16', '198.18.0.0/15', '198.51.100.0/24',
              '203.0.113.0/24', '240.0.0.0/4', '255.255.255.255/32', '::1/128', '::/128', '::ffff:0:0/96',
              '100::/64', '2001::/23', '2001:2::/48', '2001:db8::/32', '2001:10::/28', 'fc00::/7'}
publicIPs = '100.64.0.0/10'
reservedIPs = '240.0.0.0/4'
unspecifiedIPs = {'0.0.0.0', '::'}
linkLocalIPs = {'169.254.0.0/16', 'fe80::/10'}
loopBackIPs = {'127.0.0.0/8', '::1'}
ipv4Mapped = "::ffff:0:0/96"
ipv4Translated = "::ffff:0:0:0/96"
ipv4ipv6Translated = '64:ff9b::/96'
teredo = '2001::/32'
sixToFour = '2002::/16'
siteLocal = 'fc00::/7'
