from PySparkIP.src.PySparkIP.PySparkIP import *
import pytest


class TestSetMap:
    @pytest.fixture(autouse=True)
    def before_all(request, mocker):
        mocker.patch('PySparkIP.src.PySparkIP.PySparkIP.update_sets')

    def test_add_set(request):
        ip_set = IPSet()
        SparkIPSets.add(ip_set, 'ip_set')
        assert 'ip_set' in SparkIPSets.setMap

    def test_remove_set(request):
        ip_set = IPSet()
        ip_set2 = IPSet()
        SparkIPSets.add(ip_set, 'ip_set')
        SparkIPSets.add(ip_set2, 'ip_set2')

        SparkIPSets.remove('ip_set', 'ip_set2')
        # Do it again to make sure no error gets thrown
        SparkIPSets.remove('ip_set', 'ip_set2')
        assert 'ip_set' not in SparkIPSets.setMap
        assert 'ip_set2' not in SparkIPSets.setMap

    def test_clear_set(request):
        ip_set = IPSet()
        ip_set2 = IPSet()
        SparkIPSets.add(ip_set, 'ip_set')
        SparkIPSets.add(ip_set2, 'ip_set2')

        SparkIPSets.clear()

        assert 'ip_set' not in SparkIPSets.setMap
        assert 'ip_set2' not in SparkIPSets.setMap
