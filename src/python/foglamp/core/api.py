# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

import time
import re
from aiohttp import web
from foglamp import configuration_manager
from foglamp.core import scheduler_db_services, statistics_db_services
import psutil

__author__ = "Amarendra K. Sinha, Ashish Jabble"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

__start_time = time.time()

_help = """
    -------------------------------------------------------------------------------
    | GET             | /foglamp/ping                                             |

    | GET             | /foglamp/categories                                       |
    | GET             | /foglamp/category/{category_name}                         |
    | GET PUT DELETE  | /foglamp/category/{category_name}/{config_item}           |

    | GET             | /foglamp/schedule/process                                 |

    | GET             | /foglamp/schedules                                        |
    | POST            | /foglamp/schedule                                         |
    | GET PUT DELETE  | /foglamp/schedule/{schedule_id}                           |

    | GET             | /foglamp/tasks                                            |
    | GET             | /foglamp/tasks/latest                                     |
    | POST            | /foglamp/task                                             |
    | GET DELETE      | /foglamp/task/{task_id}                                   |

    | GET             | /foglamp/statistics                                       |
    | GET             | /foglamp/statistics/history                               |
    -------------------------------------------------------------------------------
"""


def _find_process_info(process_name):
    """Find Process info"""

    process = [proc for proc in psutil.process_iter() if proc.name() == process_name]

    if len(process) == 0:
        return None

    return dict({
        "pid": process[0].pid,
        "status": process[0].status(),
        "start_time": process[0].create_time(),
        "cpu_percent": process[0].cpu_percent(interval=1.0),
        "memory_percent": process[0].memory_percent()
    })


def _process_info(_now):
    """Process info"""

    process_info = _find_process_info('foglamp')
    json_data = ''
    if process_info is not None:
        uptime = _now - process_info['start_time']
        pid = process_info['pid']
        process_status = process_info['status']
        process_memory = process_info['memory_percent']
        process_cpu_percent = process_info['cpu_percent']
        json_data = {'process_info': {
                      'uptime': uptime,
                      'pid': pid,
                      'status': process_status,
                      'memory_percentage': process_memory,
                      'cpu_percentage': process_cpu_percent
                  }}

    return json_data


def _system_usage_info():
    """System info"""

    system_disk_usage = psutil.disk_usage('/')
    cpu_count = psutil.cpu_count()
    virtual_memory = psutil.virtual_memory()
    swap_memory = psutil.swap_memory()

    json_data = {'system_info': {
        'disk_usage': {
            'total': system_disk_usage.total,
            'used': system_disk_usage.used,
            'free': system_disk_usage.free,
            'percent': system_disk_usage.percent
        },
        'cpu_usage': {
            'count': cpu_count
        },
        'memory_usage': {
            'virtual_memory': {
                'total': virtual_memory.total,
                'available': virtual_memory.available,
                'used': virtual_memory.used,
                'free': virtual_memory.free,
                'percent': virtual_memory.percent,
                'active': virtual_memory.active,
                'inactive': virtual_memory.inactive,
                'cached': virtual_memory.cached,
                'shared': virtual_memory.shared,
                'buffers': virtual_memory.buffers
            },
            'swap_memory': {
                'total': swap_memory.total,
                'used': swap_memory.used,
                'free': swap_memory.free,
                'percent': swap_memory.percent
            }
        }
    }}

    return json_data

async def ping(request):
    """
    Args:
        request: accepts ?details=1 as query param to return process and system info

   Returns:
        basic health information json payload

   :Example:

       curl -X GET http://localhost:8082/foglamp/ping
       curl -X GET http://localhost:8082/foglamp/ping?details=1
    """
    _now = time.time()
    since_started = _now - __start_time
    result = {'uptime': since_started}

    if 'details' in request.query:
        detail_query_param = request.query['details']
        if detail_query_param.isdigit() and detail_query_param == '1':
            proc_info = _process_info(_now)
            result.update(proc_info)
            sys_info = _system_usage_info()
            result.update(sys_info)

    return web.json_response(result)


#################################
#  Configuration Manager
#################################

async def get_categories(request):
    """

    :param request:
    :return: the list of known categories in the configuration database
    """
    categories = await configuration_manager.get_all_category_names()
    categories_json = [{"key": c[0], "description": c[1]} for c in categories]

    return web.json_response({'categories': categories_json})


async def get_category(request):
    """

    :param request:  category_name is required
    :return: the configuration items in the given category.
    """
    category_name = request.match_info.get('category_name', None)
    category = await configuration_manager.get_category_all_items(category_name)
    # TODO: If category is None from configuration manager. Should we send category
    # as an empty array or error message in JSON format?
    if category is None:
        category = []

    return web.json_response(category)


async def get_category_item(request):
    """

    :param request: category_name & config_item are required
    :return: the configuration item in the given category.
    """
    category_name = request.match_info.get('category_name', None)
    config_item = request.match_info.get('config_item', None)
    category_item = await configuration_manager.get_category_item(category_name, config_item)
    # TODO: better error handling / info message
    if (category_name is None) or (config_item is None):
        category_item = []

    return web.json_response(category_item)


async def set_configuration_item(request):
    """

    :param request: category_name, config_item are required and For PUT request {"value" : someValue) is required
    :return: set the configuration item value in the given category.

    :Example:

        For {category_name} PURGE  update/delete value for config_item {age}

        curl -H "Content-Type: application/json" -X PUT -d '{"value":some_value}' http://localhost:8082/foglamp/category/{category_name}/{config_item}

        curl -X DELETE http://localhost:8082/foglamp/category/{category_name}/{config_item}
    """
    category_name = request.match_info.get('category_name', None)
    config_item = request.match_info.get('config_item', None)

    if request.method == 'PUT':
        data = await request.json()
        value = data['value']
    elif request.method == 'DELETE':
        value = ''

    await configuration_manager.set_category_item_value_entry(category_name, config_item, value)
    result = await configuration_manager.get_category_item(category_name, config_item)

    return web.json_response(result)


#################################
#  Scheduler Services
#################################

# Scheduled_processes

async def get_scheduled_processes(request):
    """Returns a list of all the defined scheduled_processes from scheduled_processes table"""

    try:
        processes = await scheduler_db_services.read_scheduled_processes()

        return web.json_response({'processes': processes})
    except Exception as ex:
        raise web.HTTPInternalServerError(reason='FogLAMP has encountered an internal error', text=str(ex))


# Schedules

async def get_schedules(request):
    """Returns a list of all the defined schedules from schedules table"""

    try:
        schedules = await scheduler_db_services.read_schedule()

        return web.json_response({'schedules': schedules})
    except Exception as ex:
        raise web.HTTPInternalServerError(reason='FogLAMP has encountered an internal error', text=str(ex))


async def get_schedule(request):
    """Return the information for the given schedule from schedules table"""

    try:
        schedule_id = request.match_info.get('schedule_id', None)

        if not schedule_id:
            raise ValueError('No such Schedule')

        schedule = await scheduler_db_services.read_schedule(schedule_id)

        if not schedule:
            raise ValueError('No such Schedule')

        return web.json_response(schedule)
    except ValueError as ex:
        raise web.HTTPNotFound(reason=str(ex))
    except Exception as ex:
        raise web.HTTPInternalServerError(reason='FogLAMP has encountered an internal error', text=str(ex))


async def post_schedule(request):
    """Create a new schedule in schedules table"""
    pass

async def update_schedule(request):
    """Update a schedule in schedules table"""
    pass

async def delete_schedule(request):
    """Delete a schedule from schedules table"""
    pass


# Tasks

async def get_task(request):
    """Returns a task"""

    try:
        task_id = request.match_info.get('task_id', None)

        if not task_id:
            raise ValueError('No such Task')

        task = await scheduler_db_services.read_task(task_id)

        if not task:
            raise ValueError('No such Task')

        return web.json_response(task)
    except ValueError as ex:
        raise web.HTTPNotFound(reason=str(ex))
    except Exception as ex:
        raise web.HTTPInternalServerError(reason='FogLAMP has encountered an internal error', text=str(ex))


async def get_tasks(request):
    """Returns the list of tasks"""

    try:
        task_id = None

        # TODO: Use enum in place int state
        state = request.query.get('state') if 'state' in request.query else None
        if state:
            if not re.match("(^[1-4]$)", state):
                raise ValueError('This state value not permitted')
            else:
                state = int(state)

        name = request.query.get('name') if 'name' in request.query else None

        tasks = await scheduler_db_services.read_task(task_id, state, name)

        if not tasks:
            raise ValueError('No such Tasks')

        return web.json_response({'tasks': tasks})
    except ValueError as ex:
        raise web.HTTPNotFound(reason=str(ex))
    except Exception as ex:
        raise web.HTTPInternalServerError(reason='FogLAMP has encountered an internal error', text=str(ex))

async def get_tasks_latest(request):
    """Returns the list of the most recent task execution for each name from tasks table"""

    try:
        # TODO: Use enum in place int state
        state = request.query.get('state') if 'state' in request.query else None
        if state:
            if not re.match("(^[1-4]$)", state):
                raise ValueError('This state value not permitted')
            else:
                state = int(state)

        name = request.query.get('name') if 'name' in request.query else None

        tasks = await scheduler_db_services.read_tasks_latest(state, name)

        if not tasks:
            raise ValueError('No such Task')

        return web.json_response({'tasks': tasks})
    except ValueError as ex:
        raise web.HTTPNotFound(reason=str(ex))
    except Exception as ex:
        raise web.HTTPInternalServerError(reason='FogLAMP has encountered an internal error', text=str(ex))

async def post_task(request):
    """ create a new task"""
    pass

async def cancel_task(request):
    """Cancel a running task from tasks table"""
    pass

#################################
#  Statistics
#################################

async def get_statistics(request):
    """
        Returns a general set of statistics

        Example: curl -X GET http://localhost:8082/foglamp/statistics
    """

    try:
        statistics = await statistics_db_services.read_statistics()

        return web.json_response(statistics)
    except Exception as ex:
        raise web.HTTPInternalServerError(reason='FogLAMP has encountered an internal error', text=str(ex))


async def get_statistics_history(request):
    """
        Returns a list of general set of statistics

        Example: curl -X GET -d limit=1 http://localhost:8082/foglamp/statistics/history
    """

    try:
        limit = request.query.get('limit') if 'limit' in request.query else 0

        statistics = await statistics_db_services.read_statistics_history(int(limit))

        if not statistics:
            raise ValueError('No statistics available')

        # TODO: find out where from this "interval" will be picked and what will be its role in query?
        return web.json_response({"interval": 5, 'statistics': statistics})
    except ValueError as ex:
        raise web.HTTPNotFound(reason=str(ex))
    except Exception as ex:
        raise web.HTTPInternalServerError(reason='FogLAMP has encountered an internal error', text=str(ex))
