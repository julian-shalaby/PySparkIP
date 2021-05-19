from SparkIP.IPAddressUDT import IPAddress
import ipaddress


def test_init():
    ip = IPAddress("192.168.0.1")
    assert ip.ipaddr == ipaddress.ip_address("192.168.0.1")


def test_is_ipv4_mapped_success():
    ip = IPAddress("::ffff:49e7:a9b2")
    assert ip.is_ipv4_mapped()


def test_is_ipv4_mapped_failure():
    ip = IPAddress("1::")
    assert not ip.is_ipv4_mapped()


def test_is_6to4_success():
    ip = IPAddress("2002:49e7:a9b2::")
    assert ip.is_6to4()


def test_is_6to4_failure():
    ip = IPAddress("1::")
    assert not ip.is_6to4()


def test_is_teredo_success():
    ip = IPAddress("2001:0:49e7:a9b2::")
    assert ip.is_teredo()


def test_is_teredo_failure():
    ip = IPAddress("1::")
    assert not ip.is_teredo()


def test_teredo_success():
    ip = IPAddress("2001:0:49e7:a9b2::")
    assert ip.teredo() == ipaddress.ip_address("2001:0:49e7:a9b2::").teredo


def test_teredo_failure():
    ip = IPAddress("192.168.0.1")
    assert not ip.teredo()


def test_ipv4_mapped_success():
    ip = IPAddress("::ffff:49e7:a9b2")
    assert ip.ipv4_mapped() == ipaddress.ip_address("73.231.169.178")


def test_ipv4_mapped_failure():
    ip = IPAddress("192.168.0.1")
    assert not ip.ipv4_mapped()


def test_sixtofour_success():
    ip = IPAddress("2002:49e7:a9b2::")
    assert ip.sixtofour() == ipaddress.ip_address("73.231.169.178")


def test_sixtofour_failure():
    ip = IPAddress("192.168.0.1")
    assert not ip.sixtofour()
