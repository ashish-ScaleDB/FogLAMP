# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END


import json
import pytest
from foglamp.services.core import routes
from aiohttp import web

__author__ = "Ashish Jabble"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


@pytest.allure.feature("rest-api-unit")
@pytest.allure.story("audit")
class TestAudit:

    def create_app(self, loop):
        app = web.Application(loop=loop)
        # fill the routes table
        routes.setup(app)
        return app

    async def test_get_severity(self, test_client):
        client = await test_client(self.create_app)
        resp = await client.get('/foglamp/audit/severity')
        assert resp.status == 200
        result = await resp.text()
        result_dict = json.loads(result)
        log_severity = result_dict['logSeverity']

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

    # Via Fixture
    @pytest.fixture
    def cli(self, loop, test_client):
        app = web.Application(loop=loop)
        # fill the routes table
        routes.setup(app)
        return loop.run_until_complete(test_client(app))

    async def test_get_severity_via_fixture(self, cli):
        resp = await cli.get('/foglamp/audit/severity')
        assert resp.status == 200
        result = await resp.text()
        result_dict = json.loads(result)
        log_severity = result_dict['logSeverity']

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


# O/p
"""
collected 2 items 

test_audit.py::TestAudit::test_get_severity[pyloop] PASSED
test_audit.py::TestAudit::test_get_severity_via_fixture[pyloop] PASSED

========== 2 passed in 0.19 seconds ============================
"""