# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

import asyncio
from unittest.mock import MagicMock, patch
import pytest

from foglamp.common.statistics import Statistics, _logger
from foglamp.common.storage_client.storage_client import StorageClient


__author__ = "Ashish Jabble"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


@pytest.allure.feature("unit")
@pytest.allure.story("common", "statistics")
class TestStatistics:

    def test_init_with_storage(self):
        storage_client_mock = MagicMock(spec=StorageClient)
        s = Statistics(storage_client_mock)
        assert isinstance(s._storage, StorageClient)

    def test_init_with_no_storage(self):
        storage_client_mock = None
        with pytest.raises(TypeError) as excinfo:
            Statistics(storage_client_mock)
        assert 'Must be a valid Storage object' in str(excinfo.value)

    def test_update(self):
        storage_client_mock = MagicMock(spec=StorageClient)
        s = Statistics(storage_client_mock)
        payload = '{"where": {"column": "key", "condition": "=", "value": "READING"}, ' \
                  '"expressions": [{"column": "value", "operator": "+", "value": 5}]}'
        result = {"response": "updated", "rows_affected": 1}
        with patch.object(s._storage, 'update_tbl', return_value=result) as stat_update:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(s.update('READING', 5))
            stat_update.assert_called_once_with('statistics', payload)
            assert "updated" == result['response']

    @pytest.mark.parametrize("key, value_increment, exception_name, exception_message", [
        (123456, 120, TypeError, "key must be a string"),
        ('PURGED', '120', ValueError, "value must be an integer"),
        (None, '120', TypeError, "key must be a string"),
        ('123456', '120', ValueError, "value must be an integer"),
        ('READINGS', None, ValueError, "value must be an integer")
    ])
    def test_update_with_invalid_params(self, key, value_increment, exception_name, exception_message):
        storage_client_mock = MagicMock(spec=StorageClient)
        s = Statistics(storage_client_mock)

        with pytest.raises(exception_name) as excinfo:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(s.update(key, value_increment))

        assert exception_message == str(excinfo.value)

    def test_update_exception(self):
        storage_client_mock = MagicMock(spec=StorageClient)
        s = Statistics(storage_client_mock)
        with patch.object(s._storage, 'update_tbl', side_effect=Exception()):
            with pytest.raises(Exception):
                with patch.object(_logger, 'exception') as logger_exception:
                    loop = asyncio.get_event_loop()
                    loop.run_until_complete(s.update('B', 5))
            logger_exception.assert_called_once_with('Unable to update statistics value based on statistics_key %s and value_increment %s'
                , 'B', 5)
            
    # def test_add_update(self):
    #     stat_dict = {'FOGBENCH/TEMPERATURE': 1, 'FOGBENCH/MOUSE': 1, 'FOGBENCH/SWITCH': 1,
    #                  'FOGBENCH/HUMIDITY': 1, 'FOGBENCH/WALL CLOCK': 1, 'FOGBENCH/PRESSURE': 1}
    #     storage_client_mock = MagicMock(spec=StorageClient)
    #     s = Statistics(storage_client_mock)
    #
    #     result = {"response": "updated", "rows_affected": 1}
    #     error = {'retryable': False, 'entryPoint': 'update', 'message': 'No rows where updated'}
    #     loop = asyncio.get_event_loop()
    #     # loop.run_until_complete(s.add_update(stat_dict))
    #     with patch.object(s, 'add_update', side_effect=loop.run_until_complete(s.add_update(stat_dict))):
    #         with patch.object(s._storage, 'update_tbl', side_effect=result):
    #             assert "updated" == result['response']
    #
    #     # with patch.object(s._storage, 'update_tbl', return_value=error):
    #     #     with pytest.raises(KeyError) as excinfo:
    #     #         loop = asyncio.get_event_loop()
    #     #         loop.run_until_complete(s.add_update(stat_dict))
    #     #     print("BLAAA----", str(excinfo.value))
