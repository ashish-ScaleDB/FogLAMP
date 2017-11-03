# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END


from enum import IntEnum
from aiohttp import web

from foglamp.storage.payload_builder import PayloadBuilder
from foglamp.storage.storage import Storage

__author__ = "Amarendra K. Sinha, Ashish Jabble"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_help = """
    -------------------------------------------------------------------------------
    | GET             | /foglamp/audit                                            |
    | GET             | /foglamp/audit/logcode                                    |
    | GET             | /foglamp/audit/severity                                   |
    -------------------------------------------------------------------------------
"""

# TODO: storage object comes from app['core']
_storage = None #Storage(core_management_host='0.0.0.0', core_management_port=43509)

class Severity(IntEnum):
    """Enumeration for log.severity"""
    FATAL = 1
    ERROR = 2
    WARNING = 3
    INFORMATION = 4


####################################
#  Audit Trail
####################################


async def get_audit_entries(request):
    """
    Returns a list of audit trail entries sorted with most recent first

    :Example:

        curl -X GET http://localhost:8082/foglamp/audit

        curl -X GET http://localhost:8082/foglamp/audit?limit=5

        curl -X GET http://localhost:8082/foglamp/audit?limit=5&skip=3

        curl -X GET http://localhost:8082/foglamp/audit?source=PURGE

        curl -X GET http://localhost:8082/foglamp/audit?severity=ERROR

        curl -X GET http://localhost:8082/foglamp/audit?source=LOGGN&severity=INFORMATION&limit=10
    """
    try:
        limit = request.query.get('limit') if 'limit' in request.query else 0
        offset = request.query.get('skip') if 'skip' in request.query else 0
        source = request.query.get('source') if 'source' in request.query else None
        severity = request.query.get('severity') if 'severity' in request.query else None

        # HACK: This way when we can more future we do not get an exponential
        # explosion of if statements
        complex_payload = PayloadBuilder().WHERE(['1', '=', '1'])

        if source is not None:
            complex_payload.AND_WHERE(['code', '=', source])

        if severity is not None:
            complex_payload.AND_WHERE(['level', '=', Severity[severity].value])

        complex_payload.ORDER_BY(['ts', 'desc'])

        if limit:
            complex_payload.LIMIT(int(limit))
        if offset:
            complex_payload.OFFSET(int(offset))

        # TODO: Remove print
        print(complex_payload.payload())
        results = _storage.query_tbl_with_payload('log', complex_payload.payload())

        return web.json_response({'audit': results['rows']})

    except ValueError as ex:
        raise web.HTTPNotFound(reason=str(ex))


async def get_audit_log_codes(request):
    """
    Args:
        request:

    Returns:
           an array of log codes with description

    :Example:

        curl -X GET http://localhost:8082/foglamp/audit/logcode
    """
    result = _storage.query_tbl('log_codes')

    return web.json_response({'log_code': result['rows']})


async def get_audit_log_severity(request):
    """
    Args:
        request:

    Returns:
            an array of audit severity enumeration key index values

    :Example:

        curl -X GET http://localhost:8082/foglamp/audit/severity
    """
    results = []
    for _severity in Severity:
        data = {'index': _severity.value, 'name': _severity.name}
        results.append(data)

    return web.json_response({"log_severity": results})
