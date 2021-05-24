[![license](https://img.shields.io/badge/license-Apache_2.0-blue.svg)](https://github.com/jshalaby510/PySparkIP/blob/main/LICENSE)

# PySparkIP
An API for working with IP addresses in Apache Spark. Built on top of [ipaddress](https://docs.python.org/3/library/ipaddress.html).

## Usage
  * pip install PySparkIP
  * from PySparkIP import *

## License
This project is licensed under the Apache License. Please see [LICENSE](LICENSE) file for more details.

## Tutorial
### Initialize
Before using in SparkSQL, initialize PySparkIP by passing `spark` to `SparkIPInit`, 
then define `IPAddressUDT()` in the schema.
<br/>
Optionally pass the log level as well (if left unspecified, `SparkIPInit` resets 
the log level to "WARN" and gives a warning message).
```python
from pyspark.sql import SparkSession
from PySparkIP import *

spark = SparkSession.builder.appName("ipTest").getOrCreate()
SparkIPInit(spark)
# or SparkIPInit(spark, "DEBUG"), SparkIPInit(spark, "FATAL"), etc if specifying a log level

schema = StructType([StructField("IPAddress", IPAddressUDT())])
ipDF = spark.read.json("ipFile.json", schema=schema)
ipDF.createOrReplaceTempView("IPAddresses")
```

### Functions
**Check address type**
```python
# Multicast
spark.sql("SELECT * FROM IPAddresses WHERE isMulticast(IPAddress)")
ipDF.select('*').filter("isMulticast(IPAddress)")
ipDF.select('*').withColumn("IPColumn", isMulticast("IPAddress"))

"""
Other address types:
    isPrivate, isGlobal, isUnspecified, isReserved, 
    isLoopback, isLinkLocal, isIPv4Mapped, is6to4, 
    isTeredo, isIPv4, isIPv6
"""
```

**Output address in different formats**
```python
# Exploded
spark.sql("SELECT explodedIP(IPAddress) FROM IPAddresses")
ipDF.select(explodedIP("IPAddress"))

# Compressed
spark.sql("SELECT compressedIP(IPAddress) FROM IPAddresses")
ipDF.select(compressedIP("IPAddress"))
```

**Sort IP Addresses**
```python
# SparkSQL doesn't support values > LONG_MAX
# To sort IPv6 addresses, use ipAsBinary
# To sort IPv4 addresses, use either ipv4AsNum or ipAsBinary, but ipv4AsNum is more efficient

# Sort IPv4 and IPv6
spark.sql("SELECT * FROM IPAddresses SORT BY ipAsBinary(IPAddress)")
ipDF.select('*').sort(ipAsBinary("IPAddress"))

# Sort ONLY IPv4
spark.sql("SELECT * FROM IPv4 SORT BY ipv4AsNum(IPAddress)")
ipv4DF.select('*').sort(ipv4AsNum("IPAddress"))
```

**IP network functions**
```python
# Network contains
spark.sql("SELECT * FROM IPAddresses WHERE networkContains(IPAddress, '195.0.0.0/16')")
ipDF.select('*').filter("networkContains(IPAddress, '195.0.0.0/16')")
ipDF.select('*').withColumn("netCol", networkContains("192.0.0.0/16")("IPAddress"))

# Or use ipaddress.ip_network objects
net1 = ipaddress.ip_network('::/10')
ipDF.select('*').filter(networkContains(net1)("IPAddress"))
```

**IP Set**
#### Create IP Sets (Note: This functionality also works with add and remove):
```python
# Strings
ipStr = '192.0.0.0'
netStr = '225.0.0.0'
# Tuples, lists, or sets
ip_net_mix = ('::5', '5.0.0.0/8', '111.8.9.7')
# ipaddress objects
ipAddr = ipaddress.ip_address('::')
# Dataframes
ipMulticastDF = spark.sql("SELECT IPAddress FROM IPAddresses WHERE isMulticast(IPAddress)")

""" 
Or use our predefined networks (multicastIPs, privateIPs, 
 publicIPs, reservedIPs, unspecifiedIPs, linkLocalIPs, 
 loopBackIPs, ipv4Mapped, ipv4Translated, ipv4ipv6Translated,
 teredo, sixToFour, or siteLocal)
 """

# Mix them together
ipSet = IPSet(ipStr, '::/16', '2001::', netStr, ip_net_mix, privateIPs)
ipSet2 = IPSet("6::", "9.0.8.7", ipAddr, ipMulticastDF)
# Use other IPSets
ipSet3 = IPSet(ipSet, ipSet2)
# Or just make an empty set
ipSet4 = IPSet()
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
ipDF.select('*').filter("setContains(IPAddress, 'ipSet')")
ipDF.select('*').withColumn("setCol", setContains(ipSet)("IPAddress"))

# Show sets available to use
SparkIPSets.setsAvailable()

# Remove a set
SparkIPSets.remove('ipSet')

# Clear sets available
SparkIPSets.clear()
```

#### IP Set functions (outside SparkSQL):
```python
ipSet = IPSet()

# Add
ipSet.add('0.0.0.0', '::/16')

# Remove
ipSet.remove('::/16')

# Contains
ipSet.contains('0.0.0.0')

# Clear
ipSet.clear()

# Show all
ipSet.showAll()

# Union
ipSet2 = ('2001::', '::33', 'ffff::f')
ipSet.union(ipSet2)

# Intersection
ipSet.intersects(ipSet2)

# Diff
ipSet.diff(ipSet2)

# Show All
ipSet.showAll()

# Return All
ipSet.returnAll()

# Is empty
ipSet.isEmpty()

# Compare IPSets
ipSet2 = ('2001::', '::33', 'ffff::f')
ipSet == ipSet2
ipSet != ipSet2

# Return the # of elements in the set
len(ipSet)
```
#### Other operations (outside SparkSQL):
```python
# Nets intersect
net1 = '192.0.0.0/16'
net2 = '192.0.0.0/8'
# or ipaddress.ip_network('192.0.0.0/8')
netsIntersect(net1, net2)
```
