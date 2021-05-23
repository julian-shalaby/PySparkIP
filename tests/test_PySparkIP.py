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
