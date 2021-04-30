[![license](https://img.shields.io/badge/license-Apache_2.0-blue.svg)](https://github.com/jshalaby510/PySparkIP/blob/main/LICENSE)

# PySparkIP
An API for working with IP addresses in Apache Spark. Built on top of [ipaddress](https://docs.python.org/3/library/ipaddress.html).

## Usage
  * pip install -i https://test.pypi.org/simple/ PySparkIP==1.0.2
  * from SparkIP.SparkIP import *

## License
This project is licensed under the Apache License. Please see [LICENSE](LICENSE) file for more details.

## Tutorial
### Initialize
Before using, initialize PySparkIP by passing `spark` to `SparkIPInit`
```python
from pyspark.sql import SparkSession
from src.SparkIP.SparkIP import *

spark = SparkSession.builder.appName("ipTest").getOrCreate()
SparkIPInit(spark)
```

### SparkSQL Functions
**Check address types**
```python
# Multicast
spark.sql("SELECT * FROM IPAddresses WHERE isMulticast(IPAddress)")

# Private
spark.sql("SELECT * FROM IPAddresses WHERE isPrivate(IPAddress)")

# Global
spark.sql("SELECT * FROM IPAddresses WHERE isGlobal(IPAddress)")

# Unspecified
spark.sql("SELECT * FROM IPAddresses WHERE isUnspecified(IPAddress)")

# Reserved
spark.sql("SELECT * FROM IPAddresses WHERE isReserved(IPAddress)")

# Loopback
spark.sql("SELECT * FROM IPAddresses WHERE isLoopback(IPAddress)")

# Link Local
spark.sql("SELECT * FROM IPAddresses WHERE isLinkLocal(IPAddress)")

# IPv4 Mapped
spark.sql("SELECT * FROM IPAddresses WHERE isIPv4Mapped(IPAddress)")

# 6to4
spark.sql("SELECT * FROM IPAddresses WHERE is6to4(IPAddress)")

# Teredo
spark.sql("SELECT * FROM IPAddresses WHERE isTeredo(IPAddress)")

# IPv4
spark.sql("SELECT * FROM IPAddresses WHERE isIPv4(IPAddress)")

# IPv6
spark.sql("SELECT * FROM IPAddresses WHERE isIPv6(IPAddress)")
```

**Output address in different formats**
```python
# Exploded
spark.sql("SELECT explodedIP(IPAddress) FROM IPAddresses")

# Compressed
spark.sql("SELECT compressedIP(IPAddress) FROM IPAddresses")

# Teredo
spark.sql("SELECT teredo(IPAddress) FROM IPAddresses")

# IPv4 Mapped
spark.sql("SELECT IPv4Mapped(IPAddress) FROM IPAddresses")

# 6to4
spark.sql("SELECT sixtofour(IPAddress) FROM IPAddresses")
```

**Sort or compare IP Addresses**
```python
# SparkSQL doesn't support values > LONG_MAX
# To sort or compare IPv6 addresses, use ipAsBinary
# To sort or compare IPv4 addresses, use either ipv4AsNum or ipAsBinary
# But ipv4AsNum is more efficient

# Compare
spark.sql("SELECT * FROM IPAddresses WHERE ipAsBinary(IPAddress) > ipAsBinary('192.209.45.194')")

# Sort
spark.sql("SELECT * FROM IPAddresses SORT BY ipAsBinary(IPAddress)")

# Sort ONLY IPv4
spark.sql("SELECT * FROM IPv4 SORT BY ipv4AsNum(IPAddress)")
```

**IP network functions**
```python
# Network contains
spark.sql("SELECT * FROM IPAddresses WHERE networkContains(IPAddress, '195.0.0.0/16')")
```

**IP Set**
#### Create IP Sets using:
* IP addresses 
```python
ip = ipaddress.ip_address("189.118.188.64")
ipSet = IPSet(ip)
  ```
* IP networks 
```python
net = ipaddress.ip_network('::/16')
ipSet = IPSet(ip)
  ```
* strings representing IP addresses or IP networks 
```python
ipStr = '192.0.0.0'
ipSet = IPSet(ipStr)
```
* lists, tuples, or sets containing any/all of the above
```python
setOfIPs = {"192.0.0.0", "5422:6622:1dc6:366a:e728:84d4:257e:655a", "::"}
ipSet = IPSet(setOfIPs)
```
* Or a mixture of any/all/none of the above!
```python
setOfIPs = {"192.0.0.0", "5422:6622:1dc6:366a:e728:84d4:257e:655a", "::"}
ipStr = '192.0.0.0'
net = ipaddress.ip_network('::/16')
ip = ipaddress.ip_address("189.118.188.64")
ipSet = IPSet(setOfIPs, '0.0.0.0', ipStr, net, ip)
```
#### Register IP Sets for use in SparkSQL:
Before using IP Sets in SparkSQL, register it by passing it to `SparkIPSets`
```python
ipSet = IPSet('::')
ipSet2 = IPSet()

# Pass the set, then the set name
SparkIPSets.add(ipSet, 'ipSet')
SparkIPSets.add(ipSet2, 'ipSet2')
```
#### Remove IP Sets from registered sets in SparkSQL:
```python
SparkIPSets.remove('ipSet', 'ipSet2')
```

#### Use IP Sets in SparkSQL:
```python
# Note you have to pass the variable name using SparkSQL, not the actual variable

# Initialize an IP Set
setOfIPs = {"192.0.0.0", "5422:6622:1dc6:366a:e728:84d4:257e:655a", "::"}
ipSet = IPSet(setOfIPs)

# Register it
SparkIPSets.add(ipSet, 'ipSet')

#Use it!
# Set Contains
spark.sql("SELECT * FROM IPAddresses WHERE setContains(IPAddress, 'ipSet')")

# Show sets available to use
SparkIPSets.setsAvailable()

# Remove a set
SparkIPSets.remove('ipSet')

# Clear sets available
SparkIPSets.clear()
```

#### IP Set functions (outside of SparkSQL):
```python
ipSet = IPSet()

# Add
ipSet.add('0.0.0.0', '::/16')

# Remove
ipSet.remove('::/16')

# Contains
ipSet.contains('0.0.0.0', '::')

# Clear
ipSet.clear()

# Show all
ipSet.showAll()

# Union
ipSet2 = ('2001::', '::33', 'ffff::f')
ipSet.union(ipSet2)

# Intersects
ipSet.intersects(ipSet2)

# Diff
ipSet.diff(ipSet2)

# Is empty
ipSet.isEmpty()
```
