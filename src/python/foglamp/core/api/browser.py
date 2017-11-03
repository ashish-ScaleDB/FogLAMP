# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

"""
Experimental browser for extracting reading data from the FogLAMP data buffer

Supports a number of REST API:

  http://<address>/foglamp/asset
     - Return a summary count of all asset readings
  http://<address>/foglamp/asset/{asset_code}
    - Return a set of asset readings for the given asset 
  http://<address>/foglamp/asset/{asset_code}/{reading}
    - Return a set of sensor readings for the specified asset and sensor
  http://<address>/foglamp/asset/{asset_code}/{reading}/summary
    - Return a summary (min, max and average) for the specified asset and sensor
  http://<address>/foglamp/asset/{asset_code}/{reading}/series
    - Return a time series (min, max and average) for the specified asset and
      sensor averages over seconds, minutes or hours. The selection of seconds, minutes
      or hours is done via the group query parameter

  All but the /foglamp/asset API call take a set of optional query parameters
    limit=x     Return the first x rows only
    skip=x      skip first n entries and used with limit to implemented paged interfaces
    seconds=x   Limit the data return to be less than x seconds old
    minutes=x   Limit the data returned to be less than x minutes old
    hours=x     Limit the data returned to be less than x hours old

  Note seconds, minutes and hours can not be combined in a URL. If they are then only seconds
  will have an effect.

  TODO: Improve error handling, use a connection pool
"""

import json
from aiohttp import web

from collections import OrderedDict

from foglamp.storage.payload_builder import PayloadBuilder
from foglamp.storage.storage import Storage


__author__ = "Mark Riddoch"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

# TODO: storage object comes from app['core']
_storage = None
__DEFAULT_LIMIT = 20
__DEFAULT_OFFSET = 0
__TIMESTAMP_FMT = 'YYYY-MM-DD HH24:MI:SS.MS'


def setup(app):
    """
    Add the routes for the API endpoints supported by the data browser
    """
    app.router.add_route('GET', '/foglamp/asset', asset_counts)
    app.router.add_route('GET', '/foglamp/asset/{asset_code}', asset)
    app.router.add_route('GET', '/foglamp/asset/{asset_code}/{reading}', asset_reading)
    app.router.add_route('GET', '/foglamp/asset/{asset_code}/{reading}/summary', asset_summary)
    app.router.add_route('GET', '/foglamp/asset/{asset_code}/{reading}/series', asset_averages)


async def asset_counts(request):
    """
    Browse all the assets for which we have recorded readings and
    return a readings count.

    Return the result of the Postgres query 
    SELECT asset_code, count(*) FROM readings GROUP BY asset_code;
    """

    # TODO: FOGL-643 - Aggregate with alias support needed to use payload builder
    # PayloadBuilder().AGGREGATE(["count", "*"]).GROUP_BY('asset_code')

    agg = {"operation": "count", "column": "*", "alias": "Count"}
    d = OrderedDict()
    d['aggregate'] = agg
    d['group'] = "asset_code"

    # TODO: Remove print
    print(json.dumps(d))
    results = _storage.query_tbl_with_payload('readings', json.dumps(d))

    return web.json_response(results['rows'])


async def asset(request):
    """
    Browse a particular asset for which we have recorded readings and
    return a readings with timestamps for the asset. The number of readings
    return is defaulted to a small number (20), this may be changed by supplying
    the query parameter ?limit=xx&skip=xx

    Return the result of the Postgres query 
    SELECT TO_CHAR(user_ts, '__TIMESTAMP_FMT') as "timestamp", (reading)::jsonFROM readings WHERE asset_code = 'asset_code' ORDER BY user_ts DESC LIMIT 20 OFFSET 0
    """
    asset_code = request.match_info.get('asset_code', '')

    # TODO: FOGL-637, 640
    timestamp = {"column": "user_ts", "format": __TIMESTAMP_FMT, "alias": "timestamp"}
    d = OrderedDict()
    d['return'] = [timestamp]
    d['where'] = {"column": "asset_code", "condition": "=", "value": asset_code}
    # TODO: + where_clause(request)

    # Add the order by and limit clause
    d['sort'] = {"column": "user_ts", "direction": "desc"}
    limit = int(request.query.get('limit')) if 'limit' in request.query else __DEFAULT_LIMIT
    offset = int(request.query.get('skip')) if 'skip' in request.query else __DEFAULT_OFFSET

    d['limit'] = limit
    if offset:
        d['skip'] = offset

    # TODO: Remove print
    print(json.dumps(d))
    results = _storage.query_tbl_with_payload('readings', json.dumps(d))

    return web.json_response(results['rows'])


async def asset_reading(request):
    """
    Browse a particular sensor value of a particular asset for which we have recorded readings and
    return the timestamp and reading value for that sensor. The number of rows returned
    is limited to a small number, this number may be altered by use of
    the query parameter limit=xxx&skip=xxx.

    The readings returned can also be time limited by use of the query
    parameter seconds=sss. This defines a number of seconds that the reading
    must have been processed in. Older readings than this will not be returned.

    The readings returned can also be time limited by use of the query
    parameter minutes=mmm. This defines a number of minutes that the reading
    must have been processed in. Older readings than this will not be returned.

    The readings returned can also be time limited by use of the query
    parameter hours=hh. This defines a number of hours that the reading
    must have been processed in. Older readings than this will not be returned.

    Only one of hour, minutes or seconds should be supplied

    Return the result of the Postgres query 
    SELECT TO_CHAR(user_ts, '__TIMESTAMP_FMT') as "timestamp", reading->>'reading' FROM readings WHERE asset_code = 'asset_code' ORDER BY user_ts DESC LIMIT 20 OFFSET 0
    """
    asset_code = request.match_info.get('asset_code', '')
    reading = request.match_info.get('reading', '')

    # TODO: FOGL-637, 640
    timestamp = {"column": "user_ts", "format": __TIMESTAMP_FMT, "alias": "timestamp"}
    json_property = OrderedDict()
    json_property['json'] = {"column": "reading", "properties": reading}
    json_property['alias'] = "reading"

    d = OrderedDict()
    d['return'] = [timestamp, json_property]
    d['where'] = {"column": "asset_code", "condition": "=", "value": asset_code}
    # TODO: + where_clause(request)

    # Add the order by and limit clause
    d['sort'] = {"column": "user_ts", "direction": "desc"}
    limit = int(request.query.get('limit')) if 'limit' in request.query else __DEFAULT_LIMIT
    offset = int(request.query.get('skip')) if 'skip' in request.query else __DEFAULT_OFFSET

    d['limit'] = limit
    if offset:
        d['skip'] = offset

    # TODO: Remove print
    print(json.dumps(d))
    results = _storage.query_tbl_with_payload('readings', json.dumps(d))

    return web.json_response(results)


async def asset_summary(request):
    """
    Browse all the assets for which we have recorded readings and
    return a summary for a particular sensor. The values that are
    returned are the min, max and average values of the sensor.

    The readings summarised can also be time limited by use of the query
    parameter seconds=sss. This defines a number of seconds that the reading
    must have been processed in. Older readings than this will not be summarised.

    The readings summarised can also be time limited by use of the query
    parameter minutes=mmm. This defines a number of minutes that the reading
    must have been processed in. Older readings than this will not be summarised.

    The readings summarised can also be time limited by use of the query
    parameter hours=hh. This defines a number of hours that the reading
    must have been processed in. Older readings than this will not be summarised.

    Only one of hour, minutes or seconds should be supplied

    Return the result of the Postgres query 
    SELECT MIN(reading->>'reading'), MAX(reading->>'reading'), AVG((reading->>'reading')::float) FROM readings WHERE asset_code = 'asset_code'
    """
    asset_code = request.match_info.get('asset_code', '')
    reading = request.match_info.get('reading', '')

    # TODO: FOGL-643
    prop_dict = {"column": "reading", "properties": reading}
    min_dict = {"operation": "min", "json": prop_dict, "alias": "min"}
    max_dict = {"operation": "max", "json": prop_dict, "alias": "max"}
    avg_dict = {"operation": "avg", "json": prop_dict, "alias": "average"}

    d = OrderedDict()
    d['aggregate'] = [min_dict, max_dict, avg_dict]
    d['where'] = {"column": "asset_code", "condition": "=", "value": asset_code}
    # TODO: + where_clause(request)

    # TODO: Remove print
    print(json.dumps(d))
    results = _storage.query_tbl_with_payload('readings', json.dumps(d))

    return web.json_response({reading: results})


async def asset_averages(request):
    """
    Browse all the assets for which we have recorded readings and
    return a series of averages per second, minute or hour.

    The readings averaged can also be time limited by use of the query
    parameter seconds=sss. This defines a number of seconds that the reading
    must have been processed in. Older readings than this will not be summarised.

    The readings averaged can also be time limited by use of the query
    parameter minutes=mmm. This defines a number of minutes that the reading
    must have been processed in. Older readings than this will not be summarised.

    The readings averaged can also be time limited by use of the query
    parameter hours=hh. This defines a number of hours that the reading
    must have been processed in. Older readings than this will not be summarised.

    Only one of hour, minutes or seconds should be supplied

    The amount of time covered by each returned value is set using the
    query parameter group. This may be set to seconds, minutes or hours

    Return the result of the Postgres query 
    SELECT user_ts AVG((reading->>'reading')::float) FROM readings WHERE asset_code = 'asset_code' GROUP BY user_ts
    """
    asset_code = request.match_info.get('asset_code', '')
    reading = request.match_info.get('reading', '')

    ts_restraint = 'YYYY-MM-DD HH24:MI:SS'
    if 'group' in request.query:
        if request.query['group'] == 'seconds':
            ts_restraint = 'YYYY-MM-DD HH24:MI:SS'
        elif request.query['group'] == 'minutes':
            ts_restraint = 'YYYY-MM-DD HH24:MI'
        elif request.query['group'] == 'hours':
            ts_restraint = 'YYYY-MM-DD HH24'

    # TODO: FOGL-637, 640
    timestamp = {"column": "user_ts", "format": ts_restraint, "alias": "timestamp"}
    prop_dict = {"column": "reading", "properties": reading}
    min_dict = {"operation": "min", "json": prop_dict, "alias": "min"}
    max_dict = {"operation": "max", "json": prop_dict, "alias": "max"}
    avg_dict = {"operation": "avg", "json": prop_dict, "alias": "average"}

    agg = OrderedDict()
    agg['aggregate'] = [min_dict, max_dict, avg_dict]
    d = OrderedDict()
    d['return'] = [timestamp, agg]
    d['where'] = {"column": "asset_code", "condition": "=", "value": asset_code}
    # TODO: + where_clause(request)

    # TODO: Below payload is NOT COMPLETED
    # Add the group by
    # query += """ GROUP BY TO_CHAR(user_ts, '{0}') ORDER BY 1""".format(ts_restraint)

    # Add the order by and limit clause
    limit = int(request.query.get('limit')) if 'limit' in request.query else __DEFAULT_LIMIT
    d['limit'] = limit

    # TODO: Remove print
    print(json.dumps(d))
    results = _storage.query_tbl_with_payload('readings', json.dumps(d))

    return web.json_response(results)


def _where_clause(request):
    where_clause = ''

    if 'seconds' in request.query:
        where_clause += """ AND user_ts > NOW() - INTERVAL '{0} seconds'""".format(request.query['seconds'])
    elif 'minutes' in request.query:
        where_clause += """ AND user_ts > NOW() - INTERVAL '{0} minutes'""".format(request.query['minutes'])
    elif 'hours' in request.query:
        where_clause += """ AND user_ts > NOW() - INTERVAL '{0} hours'""".format(request.query['hours'])

    return where_clause
