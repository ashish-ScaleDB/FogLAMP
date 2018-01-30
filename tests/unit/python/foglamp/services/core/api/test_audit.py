# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END


import json
import pytest
from aiohttp import web
from unittest.mock import MagicMock, patch
from foglamp.services.core import routes
from foglamp.services.core import connect
from foglamp.common.storage_client.storage_client import StorageClient


__author__ = "Ashish Jabble"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


@pytest.allure.feature("rest-api-unit")
@pytest.allure.story("audit")
class TestAudit:

    @pytest.fixture
    def client(self, loop, test_client):
        app = web.Application(loop=loop)
        # fill the routes table
        routes.setup(app)
        return loop.run_until_complete(test_client(app))

    async def test_get_severity(self, client):
        resp = await client.get('/foglamp/audit/severity')
        assert resp.status == 200
        result = await resp.text()
        json_response = json.loads(result)
        log_severity = json_response['logSeverity']

        # verify the severity count
        assert 4 == len(log_severity)

        # verify the name and value of severity
        for i in range(len(log_severity)):
            if log_severity[i]['index'] == 1:
                assert 1 == log_severity[i]['index']
                assert 'FATAL' == log_severity[i]['name']
            elif log_severity[i]['index'] == 2:
                assert 2 == log_severity[i]['index']
                assert 'ERROR' == log_severity[i]['name']
            elif log_severity[i]['index'] == 3:
                assert 3 == log_severity[i]['index']
                assert 'WARNING' == log_severity[i]['name']
            elif log_severity[i]['index'] == 4:
                assert 4 == log_severity[i]['index']
                assert 'INFORMATION' == log_severity[i]['name']

    async def test_audit_log_codes(self, client):
        storage_client_mock = MagicMock(StorageClient)
        response = [{"code": "PURGE", "description": "Data Purging Process"},
                    {"code": "LOGGN", "description": "Logging Process"},
                    {"code": "STRMN", "description": "Streaming Process"},
                    {"code": "SYPRG", "description": "System Purge"}
                    ]
        with patch.object(connect, 'get_storage', return_value=storage_client_mock):
            with patch.object(storage_client_mock, 'query_tbl', return_value={"rows": response}):
                resp = await client.get('/foglamp/audit/logcode')
                assert resp.status == 200
                result = await resp.text()
                json_response = json.loads(result)
                log_codes = [key['code'] for key in json_response['logCode']]

                # verify the default log_codes which are defined in init.sql
                assert 4 == len(log_codes)

                # verify code values
                assert 'PURGE' in log_codes
                assert 'LOGGN' in log_codes
                assert 'STRMN' in log_codes
                assert 'SYPRG' in log_codes
