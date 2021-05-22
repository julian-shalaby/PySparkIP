from SparkIP.SparkIP import IPSet
from pytest-mock import mocker

@pytest.fixture(scope="session", autouse=True)
def before_all(request):
    mocker.patch('SparkIP.SparkIP.update_sets')


# def add(self, *ips):
#         # Iterate through all IPs passed through
#         for ip in ips:
#             # If its an IP UDT, extract the UDTs value, add it to the map, then go to the next iteration
#             if isinstance(ip, IPAddress):
#                 self.ipMap[str(ip.ipaddr)] = int(ip.ipaddr)
#                 continue

#             # If its an IPSet, extract all of its values to a list and add it to the set
#             if isinstance(ip, IPSet):
#                 ip = ip.returnAll()

#             # If its a list, tuple, or set, iterate through it and add each element to the set
#             if type(ip) is list or type(ip) is tuple or type(ip) is set:
#                 # Try converting each element to an ipaddress object.
#                 # If it succeeds, add it to the IP address map
#                 for element in ip:
#                     try:
#                         ipaddr = ipaddress.ip_address(element)
#                         self.ipMap[str(ipaddr)] = int(ipaddr)
#                     # If it fails, it is a network. Add it to the IP network tree
#                     except:
#                         self.root = self.netAVL.insert(self.root, ipaddress.ip_network(element))
#                 # Continue to the next input after iterating through the list
#                 continue

#             # If its not a list, tuple, set, or UDT, try converting it to an ipaddress object.
#             # If it succeeds, add it to the IP address map
#             try:
#                 ipaddr = ipaddress.ip_address(ip)
#                 self.ipMap[str(ipaddr)] = int(ipaddr)
#             # If it fails, it is a network. Add it to the IP network tree
#             except:
#                 self.root = self.netAVL.insert(self.root, ipaddress.ip_network(ip))

#         # Re-register the IPSet UDF or else this set won't be updated in the UDF
#         update_sets()

    # def remove(self, *ips):
    #     # Iterate through all IPs passed through
    #     for ip in ips:
    #         # If its an IP UDT, extract the UDTs value, remove it to from map, then go to the next iteration
    #         if isinstance(ip, IPAddress):
    #             del self.ipMap[str(ip.ipaddr)]
    #             continue

    #         # If its a list, tuple, or set, iterate through it and remove each element from the set
    #         if type(ip) is list or type(ip) is tuple or type(ip) is set:
    #             # Try converting each element to an ipaddress object.
    #             # If it succeeds, remove it from the IP address map
    #             for element in ip:
    #                 try:
    #                     del self.ipMap[str(ipaddress.ip_address(element))]
    #                 # If it fails, it is a network. Remove it from the IP network tree
    #                 except:
    #                     self.root = self.netAVL.delete(self.root, ipaddress.ip_network(element))
    #             # Continue to the next input after iterating through the list
    #             continue

    #         # If its not a list, tuple, set, or UDT, try converting it to an ipaddress object.
    #         # If it succeeds, remove it from the IP address map
    #         try:
    #             del self.ipMap[str(ipaddress.ip_address(ip))]
    #         except:
    #             # If it fails, it is a network. Remove it from the IP network tree
    #             self.root = self.netAVL.delete(self.root, ipaddress.ip_network(ip))

    #     # Re-register the IPSet UDF or else this set won't be updated in the UDF
    #     update_sets()

    # def contains(self, *ips):
    #     # Iterate through all IPs passed through
    #     for ip in ips:
    #         # Flag to check if any of the IPs passed through are false
    #         # If any are false, return false
    #         found = False
    #         # If its an IP UDT, extract the UDTs value, check if its in the set, then go to the next iteration
    #         if isinstance(ip, IPAddress):
    #             # Check if its in the IP address hash map. Go to next iter if it is
    #             if str(ip.ipaddr) in self.ipMap:
    #                 continue
    #             # Check if its in the IP network tree. Go to next iter if it is
    #             if AVL_Tree.avl_search_ipAddr(self.root, ip.ipaddr) is True:
    #                 continue
    #             # If its not in the hash map or tree, its not found. Return false
    #             if found is False:
    #                 return False

    #         # If its a list, tuple, or set, iterate through it and check if each element is in the set
    #         if type(ip) is list or type(ip) is tuple or type(ip) is set:
    #             # Try converting each element to an ipaddress object.
    #             # If it succeeds, check if its in the set
    #             for element in ip:
    #                 # If its in the hash map or tree, its found. Go to the next iteration
    #                 # Otherwise its not found. Return False
    #                 try:
    #                     ipAddr = ipaddress.ip_address(element)
    #                     if str(ipAddr) in self.ipMap:
    #                         continue
    #                     if AVL_Tree.avl_search_ipAddr(self.root, ipAddr) is True:
    #                         continue
    #                     if found is False:
    #                         return False
    #                 # If it fails to convert, it is a network. Check if its in the IP network tree
    #                 except:
    #                     # If found, continue to next iter. If not found, return false
    #                     if AVL_Tree.avl_search(self.root, ipaddress.ip_network(element)) is True:
    #                         continue
    #                     if found is False:
    #                         return False

    #         # If its not a list, tuple, set, or UDT, try converting it to an ipaddress object.
    #         # If it succeeds, check if its in the set. Return false if its not
    #         try:
    #             ipAddr = ipaddress.ip_address(ip)
    #             if str(ipAddr) in self.ipMap:
    #                 continue
    #             if AVL_Tree.avl_search_ipAddr(self.root, ipAddr) is True:
    #                 continue
    #             if found is False:
    #                 return False
    #         # If it fails to convert, it is a network. Check if its in the IP network tree
    #         except:
    #             # If found, continue to next iter. If not found, return false
    #             if AVL_Tree.avl_search(self.root, ipaddress.ip_network(ip)) is True:
    #                 continue
    #             if found is False:
    #                 return False

    #     # If every item passed through is in the set, return true
    #     return True

    # # Remove every item from the set
    # def clear(self):
    #     self.ipMap = {}
    #     self.root = None
    #     self.netAVL = AVL_Tree()
    #     update_sets()

    # # Show every address and network in the set
    # def showAll(self):
    #     print('IP addresses:')
    #     for i in self.ipMap.keys():
    #         print(i)
    #     print('IP networks:')
    #     self.netAVL.preOrder(self.root)

    # def returnAll(self):
    #     set_list = []
    #     for i in self.ipMap.keys():
    #         set_list.append(i)
    #     set_list.extend(self.netAVL.returnAll(self.root))
    #     return set_list

    # # Check if the set is empty
    # def isEmpty(self):
    #     if not self.ipMap and self.root is None:
    #         return True
    #     return False

    # # Get the set intersection
    # def intersects(self, set2):
    #     intersectSet = IPSet()
    #     for i in self.ipMap.keys():
    #         if set2.contains(i):
    #             intersectSet.add(i)
    #     intersectSet.add(self.netAVL.netIntersect(self.root, set2))
    #     return intersectSet

    # # Get the union of 2 sets
    # def union(self, set2):
    #     unionSet = IPSet()
    #     for i in self.ipMap.keys():
    #         unionSet.add(i)
    #     for i in set2.ipMap.keys():
    #         unionSet.add(i)

    #     unionSet.add(self.netAVL.returnAll(self.root))
    #     unionSet.add(set2.netAVL.returnAll(set2.root))
    #     return unionSet

    # # Get the diff of 2 sets
    # def diff(self, set2):
    #     diffSet = IPSet()
    #     for i in self.ipMap.keys():
    #         if set2.contains(i) is False:
    #             diffSet.add(i)

    #     diffSet.add(self.netAVL.returnAll(self.root))
    #     diffSet.remove(self.netAVL.netIntersect(self.root, set2))

    #     return diffSet