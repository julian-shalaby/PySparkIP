from PySparkIP.PySparkIP import *
import pytest


class TestSetMap:
    @pytest.fixture(autouse=True)
    def before_all(request, mocker):
        mocker.patch('PySparkIP.PySparkIP.update_sets')

    def test_add_set(request):
        ip_set = IPSet()
        PySparkIPSets.add(ip_set, 'ip_set')
        assert 'ip_set' in PySparkIPSets.setMap

    def test_remove_set(request):
        ip_set = IPSet()
        ip_set2 = IPSet()
        PySparkIPSets.add(ip_set, 'ip_set')
        PySparkIPSets.add(ip_set2, 'ip_set2')

        PySparkIPSets.remove('ip_set', 'ip_set2')
        # Do it again to make sure no error gets thrown
        PySparkIPSets.remove('ip_set', 'ip_set2')
        assert 'ip_set' not in PySparkIPSets.setMap
        assert 'ip_set2' not in PySparkIPSets.setMap

    def test_clear_set(request):
        ip_set = IPSet()
        ip_set2 = IPSet()
        PySparkIPSets.add(ip_set, 'ip_set')
        PySparkIPSets.add(ip_set2, 'ip_set2')

        PySparkIPSets.clear()

        assert 'ip_set' not in PySparkIPSets.setMap
        assert 'ip_set2' not in PySparkIPSets.setMap
