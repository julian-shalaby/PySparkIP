from PySparkIP.PySparkIP import *
import pytest


class TestIPSet:
    @pytest.fixture(autouse=True)
    def before_all(request, mocker):
        mocker.patch('PySparkIP.PySparkIP.update_sets')

    @pytest.fixture(autouse=True)
    def ip_set(request):
        return IPSet()

    def test_add_ip_success(request, ip_set):
        ip = IPAddress("192.168.0.1")
        ip_set.add(ip)
        assert ip_set.contains(ip)

    def test_add_set_success(request, ip_set):
        ip_set = IPSet()
        ip = "192.168.0.1"
        ip_set.add(ip)
        assert ip_set.contains(ip)

    def test_add_list_success(request, ip_set):
        ip_set.add("192.168.0.1", "192.168.0.2")
        assert ip_set.contains("192.168.0.1")
        assert ip_set.contains("192.168.0.2")

    def test_remove_ip_success(request, ip_set):
        ip_set.add("192.168.0.1")
        ip_set.remove("192.168.0.1")
        assert ip_set.isEmpty()

    def test_remove_set_success(request, ip_set):
        ip_set.add(IPSet("192.168.0.1"))
        ip_set.remove(IPSet("192.168.0.1"))
        assert ip_set.isEmpty()

    def test_remove_list_success(request, ip_set):
        ip_set.add("192.168.0.1", "192.168.0.2")
        ip_set.remove("192.168.0.1", "192.168.0.2")
        assert ip_set.isEmpty()

    def test_clear_success(request, ip_set):
        ip_set.add("192.168.0.1")
        ip_set.clear()
        assert ip_set.isEmpty()

    def test_returnAll_success(request, ip_set):
        ip_set.add("192.168.0.1")
        result = ip_set.returnAll()

        expected_list = ["192.168.0.1"]

        for expected_item, result_item in zip(expected_list, result):
            assert expected_item == result_item

    def test_isEmpty_success(request, ip_set):
        assert ip_set.isEmpty()

    def test_equals_success(request, ip_set):
        ip_set2 = IPSet(ip_set)
        assert ip_set == ip_set2

    def test_not_equals_success(request, ip_set):
        ip_set2 = IPSet("::")
        assert ip_set != ip_set2

    def test_len(request, ip_set):
        ip_set = IPSet("::", '::/8', '192.0.0.0/8', '192.0.4.5', '225.0.0.0/16', '0.0.0.0/17', '::', '::/8')
        ip_set.remove("::/8")
        assert len(ip_set) == 5

    def test_intersection_success(request, ip_set):
        ip_set.add("::", "2001::", "192.0.0.0/16", "1.0.0.0/8", "5::")
        ip_set2 = IPSet("::", "5::", "1.0.0.0/8", "::/16", "2::")
        assert ip_set.intersection(ip_set2) == IPSet("::", "5::", "1.0.0.0/8")

    def test_union_success(request, ip_set):
        ip_set.add("::", "2001::", "192.0.0.0/16", "1.0.0.0/8", "5::")
        ip_set2 = IPSet("::", "5::", "1.0.0.0/8", "::/16", "2::")
        assert ip_set.union(ip_set2) == IPSet("::", "5::", "1.0.0.0/8", "2001::", "192.0.0.0/16", "2::", "::/16")

    def test_diff_success(request, ip_set):
        ip_set.add("::", "2001::", "192.0.0.0/16", "1.0.0.0/8", "5::")
        ip_set2 = IPSet("::", "5::", "1.0.0.0/8", "::/16", "2::")
        assert ip_set.diff(ip_set2) == IPSet("2001::", "192.0.0.0/16")

    def test_contains_success(request, ip_set):
        ip_set.add("::", "2001::", "192.0.0.0/16", "1.0.0.0/8", "5::")
        assert ip_set.contains("5::")
        assert ip_set.contains("192.0.5.0")
        assert ip_set.contains("192.0.0.0/16")

    def test_nets_intersect(self):
        net1 = '192.0.0.0/16'
        net2 = '192.0.0.0/8'
        assert netsIntersect(net1, net2)
