from .IPAddressUDT import *

"""
NEED FUNCTIONS FOR:
    Checking if networks intersect
    Better IPv4/IPv6 interface stuff (like teredo)
    IP network range format

OTHER STUFF WE NEED:
    Allow IPSets to input other IPSets for initialization, add, and remove
    Testing
    Ignore SimpleFunctionRegistry warning without messing with logLevel

USEFUL LINKS:
    (Features)
    https://docs.python.org/3/library/ipaddress.html#network-objects
    https://docs.google.com/document/d/1sLqO8XbOik4qhzOTKXiqHcFBwVOrdQnQVwyFSw5Jwc0/mobilebasic#

    (Publishing)
    https://spark-packages.org/artifact-help
    https://github.com/databricks/sbt-spark-package
    https://spark-packages.org
    https://packaging.python.org/tutorials/packaging-projects/
    https://realpython.com/pypi-publish-python-package/
"""

# Helper function for ordering IP networks in the AVL tree
# 1 = net1 > net2
# -1 = net1 < net2
# 0 = net1 == net2
def compareNetworks(net1, net2):
    if net1 is None:
        return 1
    if net2 is None:
        return -1

    netAddr1 = int(net1.network_address)
    netAddr2 = int(net2.network_address)
    if netAddr1 > netAddr2:
        return 1
    elif netAddr1 < netAddr2:
        return -1
    else:
        broadAddr1 = int(net1.broadcast_address)
        broadAddr2 = int(net2.broadcast_address)
        if broadAddr1 > broadAddr2:
            return 1
        elif broadAddr1 < broadAddr2:
            return -1
        else:
            if net1.version > net2.version:
                return 1
            elif net1.version < net2.version:
                return -1
            else:
                return 0


# Based off https://www.geeksforgeeks.org/avl-tree-set-1-insertion/
class TreeNode(object):
    def __init__(self, val):
        self.val = val
        self.left = None
        self.right = None
        self.height = 1

# AVL Tree to store IP networks
# Sorting priority order: Network address -> Broadcast address -> Network version (v6 > v4)
# If IP network is exact same, do nothing
class AVL_Tree(object):
    def __init__(self):
        self.length = 0

    def insert(self, root, key):
        if not root:
            self.length += 1
            return TreeNode(key)
        elif compareNetworks(key, root.val) == -1:
            root.left = self.insert(root.left, key)
        elif compareNetworks(key, root.val) == 1:
            root.right = self.insert(root.right, key)
        else:
            return root

        root.height = 1 + max(self.getHeight(root.left),
                              self.getHeight(root.right))

        balance = self.getBalance(root)

        if balance > 1 and compareNetworks(key, root.left.val) == -1:
            return self.rightRotate(root)

        if balance < -1 and compareNetworks(key, root.right.val) == 1:
            return self.leftRotate(root)

        if balance > 1 and compareNetworks(key, root.left.val) == 1:
            root.left = self.leftRotate(root.left)
            return self.rightRotate(root)

        if balance < -1 and compareNetworks(key, root.right.val) == -1:
            root.right = self.rightRotate(root.right)
            return self.leftRotate(root)

        return root

    def getMinValueNode(self, root):
        if root is None or root.left is None:
            return root

        return self.getMinValueNode(root.left)

    def delete(self, root, key):
        if not root:
            return root

        elif compareNetworks(key, root.val) == -1:
            root.left = self.delete(root.left, key)

        elif compareNetworks(key, root.val) == 1:
            root.right = self.delete(root.right, key)

        else:
            if root.left is None:
                temp = root.right
                root = None
                self.length -= 1
                return temp

            elif root.right is None:
                temp = root.left
                root = None
                self.length -= 1
                return temp

            temp = self.getMinValueNode(root.right)
            root.val = temp.val
            root.right = self.delete(root.right,
                                     temp.val)

        if root is None:
            return root

        root.height = 1 + max(self.getHeight(root.left),
                              self.getHeight(root.right))

        balance = self.getBalance(root)

        if balance > 1 and self.getBalance(root.left) >= 0:
            return self.rightRotate(root)

        if balance < -1 and self.getBalance(root.right) <= 0:
            return self.leftRotate(root)

        if balance > 1 and self.getBalance(root.left) < 0:
            root.left = self.leftRotate(root.left)
            return self.rightRotate(root)

        if balance < -1 and self.getBalance(root.right) > 0:
            root.right = self.rightRotate(root.right)
            return self.leftRotate(root)

        return root

    def leftRotate(self, z):
        y = z.right
        T2 = y.left

        y.left = z
        z.right = T2

        z.height = 1 + max(self.getHeight(z.left),
                           self.getHeight(z.right))
        y.height = 1 + max(self.getHeight(y.left),
                           self.getHeight(y.right))

        return y

    def rightRotate(self, z):
        y = z.left
        T3 = y.right

        y.right = z
        z.left = T3

        z.height = 1 + max(self.getHeight(z.left),
                           self.getHeight(z.right))
        y.height = 1 + max(self.getHeight(y.left),
                           self.getHeight(y.right))
        return y

    def getHeight(self, root):
        if not root:
            return 0
        return root.height

    def getBalance(self, root):
        if not root:
            return 0
        return self.getHeight(root.left) - self.getHeight(root.right)

    def preOrder(self, root):
        if not root:
            return
        print(f"{root.val}")
        self.preOrder(root.left)
        self.preOrder(root.right)

    def netIntersect(self, root, set2):
        if root is None:
            return
        temp = root
        intersectList = []
        while True:
            if set2.contains(temp.val):
                intersectList.append(temp.val)

            if temp.left is not None:
                temp = temp.left
            elif temp.right is not None:
                temp = temp.right
            else:
                return intersectList

    def returnAll(self, root):
        if root is None:
            return
        temp = root
        return_list = []
        while True:
            return_list.append(temp.val)
            if temp.left is not None:
                temp = temp.left
            elif temp.right is not None:
                temp = temp.right
            else:
                return return_list


# To check if an IP network is in the tree
def avl_search(root, key):
    if root is None or key is None:
        return False
    elif compareNetworks(key, root.val) == -1:
        return avl_search(root.left, key)
    elif compareNetworks(key, root.val) == 1:
        return avl_search(root.right, key)
    else:
        return True


# To check if an IP address is in the tree
def avl_search_ipAddr(root, key):
    if root is None or key is None:
        return False
    elif key in root.val:
        return True
    elif int(key) < int(root.val.network_address):
        return avl_search_ipAddr(root.left, key)
    elif int(key) > int(root.val.network_address):
        return avl_search_ipAddr(root.right, key)


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

    def contains(self, *ips):
        # Iterate through all IPs passed through
        for ip in ips:
            # Flag to check if any of the IPs passed through are false
            # If any are false, return false
            found = False
            # If its an IP UDT, extract the UDTs value, check if its in the set, then go to the next iteration
            if isinstance(ip, IPAddress):
                # Check if its in the IP address hash map. Go to next iter if it is
                if str(ip.ipaddr) in self.ipMap:
                    continue
                # Check if its in the IP network tree. Go to next iter if it is
                if avl_search_ipAddr(self.root, ip.ipaddr) is True:
                    continue
                # If its not in the hash map or tree, its not found. Return false
                if found is False:
                    return False

            # If its a list, tuple, or set, iterate through it and check if each element is in the set
            if type(ip) is list or type(ip) is tuple or type(ip) is set:
                # Try converting each element to an ipaddress object.
                # If it succeeds, check if its in the set
                for element in ip:
                    # If its in the hash map or tree, its found. Go to the next iteration
                    # Otherwise its not found. Return False
                    try:
                        ipAddr = ipaddress.ip_address(element)
                        if str(ipAddr) in self.ipMap:
                            continue
                        if avl_search_ipAddr(self.root, ipAddr) is True:
                            continue
                        if found is False:
                            return False
                    # If it fails to convert, it is a network. Check if its in the IP network tree
                    except:
                        # If found, continue to next iter. If not found, return false
                        if avl_search(self.root, ipaddress.ip_network(element)) is True:
                            continue
                        if found is False:
                            return False

            # If its not a list, tuple, set, or UDT, try converting it to an ipaddress object.
            # If it succeeds, check if its in the set. Return false if its not
            try:
                ipAddr = ipaddress.ip_address(ip)
                if str(ipAddr) in self.ipMap:
                    continue
                if avl_search_ipAddr(self.root, ipAddr) is True:
                    continue
                if found is False:
                    return False
            # If it fails to convert, it is a network. Check if its in the IP network tree
            except:
                # If found, continue to next iter. If not found, return false
                if avl_search(self.root, ipaddress.ip_network(ip)) is True:
                    continue
                if found is False:
                    return False

        # If every item passed through is in the set, return true
        return True

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

        unionSet.add(self.netAVL.netIntersect(self.root, self))
        unionSet.add(set2.netAVL.netIntersect(set2.root, set2))
        return unionSet

    # Get the diff of 2 sets
    def diff(self, set2):
        diffSet = IPSet()
        for i in self.ipMap.keys():
            if set2.contains(i) is False:
                diffSet.add(i)

        diffSet.add(self.netAVL.netIntersect(self.root, self))
        diffSet.remove(self.netAVL.netIntersect(self.root, set2))

        return diffSet


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
    def remove(self, *set_name):
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
def SparkIPInit(spark, log_level="WARN"):
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
    # IPv4 Mapped
    spark.udf.register("IPv4Mapped", lambda ip: ip.ipv4_mapped(), "string")
    # 6to4
    spark.udf.register("sixtofour", lambda ip: ip.sixtofour(), "string")
    # Teredo
    spark.udf.register("teredo", lambda ip: ip.teredo(), "string")

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
    """Set functions"""
    # Set contains
    update_sets.spark = spark or update_sets.spark
    update_sets.log_level = log_level or update_sets.log_level
    update_sets.spark.sparkContext.setLogLevel("FATAL")
    update_sets.spark.udf.register("setContains", lambda ip, ip_set: SparkIPSets.setMap[ip_set].contains(ip), "boolean")
    update_sets.spark.sparkContext.setLogLevel(log_level)
