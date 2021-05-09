import ipaddress

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

class TreeNode(object):
    def __init__(self, val):
        self.val = val
        self.left = None
        self.right = None
        self.height = 1


class AVL_Tree(object):
    def insert(self, root, key):
        if not root:
            return TreeNode(key)
        elif compareNetworks(key, root.val) == -1:
            root.left = self.insert(root.left, key)
        else:
            root.right = self.insert(root.right, key)

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
                return temp

            elif root.right is None:
                temp = root.left
                root = None
                return temp

            temp = self.getMinValueNode(root.right)
            root.val = temp.val
            root.right = self.delete(root.right,
                                     temp.val)

        # If the tree has only one node,
        # simply return it
        if root is None:
            return root

        # Step 2 - Update the height of the
        # ancestor node
        root.height = 1 + max(self.getHeight(root.left),
                              self.getHeight(root.right))

        # Step 3 - Get the balance factor
        balance = self.getBalance(root)

        # Step 4 - If the node is unbalanced,
        # then try out the 4 cases
        # Case 1 - Left Left
        if balance > 1 and self.getBalance(root.left) >= 0:
            return self.rightRotate(root)

        # Case 2 - Right Right
        if balance < -1 and self.getBalance(root.right) <= 0:
            return self.leftRotate(root)

        # Case 3 - Left Right
        if balance > 1 and self.getBalance(root.left) < 0:
            root.left = self.leftRotate(root.left)
            return self.rightRotate(root)

        # Case 4 - Right Left
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
        print("{0} ".format(root.val), end="")
        self.preOrder(root.left)
        self.preOrder(root.right)


def avl_search(root, key):
    if root is None:
        return False
    elif compareNetworks(key, root.val) == -1:
        return avl_search(root.left, key)
    elif compareNetworks(key, root.val) == 1:
        return avl_search(root.right, key)
    else:
        return True


def avl_search_ipAddr(root, key):
    if root is None:
        return False
    elif key in root.val:
        return True
    elif int(key) < int(root.val.network_address):
        return avl_search_ipAddr(root.left, key)
    elif int(key) > int(root.val.network_address):
        return avl_search_ipAddr(root.right, key)


myTree = AVL_Tree()
root = None

ipNet1 = ipaddress.ip_network('1.0.0.0/16')
ipNet2 = ipaddress.ip_network('192.0.0.0/8')
ipNet3 = ipaddress.ip_network('::/32')
ipNet4 = ipaddress.ip_network('225.0.0.0/17')
ipNet5 = ipaddress.ip_network('200::/8')

root = myTree.insert(root, ipNet1)
root = myTree.insert(root, ipNet2)
root = myTree.insert(root, ipNet3)
root = myTree.insert(root, ipNet4)
root = myTree.insert(root, ipNet5)

root = myTree.delete(root, ipNet2)
root = myTree.delete(root, ipNet2)

ip = ipaddress.ip_address('224.0.5.0')
print(avl_search_ipAddr(root, ip))
