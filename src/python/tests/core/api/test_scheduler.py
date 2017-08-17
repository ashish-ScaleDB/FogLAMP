# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

import time
import json

import aiopg
import asyncpg
import requests
import pytest
import asyncio


__author__ = "Amarendra K Sinha"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

# Module attributes
__DB_NAME = "foglamp"


async def add_test_data():
    conn = await asyncpg.connect(database=__DB_NAME)

    await conn.execute('delete from foglamp.tasks')
    await conn.execute('delete from foglamp.schedules')
    await conn.execute('delete from foglamp.scheduled_processes')
    await conn.execute('''insert into foglamp.scheduled_processes(name, script)
        values('sleep1', '["sleep", "1"]')''')
    await conn.execute('''insert into foglamp.scheduled_processes(name, script)
        values('sleep10', '["sleep", "10"]')''')
    await conn.execute('''insert into foglamp.scheduled_processes(name, script)
        values('sleep30', '["sleep", "30"]')''')
    await conn.execute('''insert into foglamp.scheduled_processes(name, script)
        values('sleep5', '["sleep", "5"]')''')


async def remove_test_data():
    pass

class TestScheduler:
    # TODO: Add tests for negative cases. There would be around 4 neagtive test cases for most of the schedule+task methods.
    # Currently only positive test cases have been added.

    @pytest.mark.asyncio
    async def test_get_scheduled_processes(self):
        # TODO: Move this code block to a proper setup method
        await add_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "start"])
        await asyncio.sleep(4)

        r = requests.get('http://localhost:8082/foglamp/schedule/process')

        retval = dict(r.json())

        assert 200 == r.status_code
        assert 'sleep30' in retval['processes']
        assert 'sleep10' in retval['processes']
        assert 'sleep5' in retval['processes']
        assert 'sleep1' in retval['processes']

        # TODO: Move this code block to a proper teardown method
        await remove_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "stop"])
        await asyncio.sleep(4)


    @pytest.mark.asyncio
    async def test_get_scheduled_process(self):
        # TODO: Move this code block to a proper setup method
        await add_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "start"])
        await asyncio.sleep(4)

        r = requests.get('http://localhost:8082/foglamp/schedule/process/sleep1')
        assert 200 == r.status_code
        assert 'sleep1' == r.json()

        # TODO: Move this code block to a proper teardown method
        await remove_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "stop"])
        await asyncio.sleep(4)


    @pytest.mark.asyncio
    async def test_post_schedule(self):
        # TODO: Move this code block to a proper setup method
        await add_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "start"])
        await asyncio.sleep(4)

        headers = {"Content-Type": 'application/json'}
        data = {"type": 3, "name": "test_post_sch", "process_name": "sleep30", "repeat": "45"}

        r = requests.post('http://localhost:8082/foglamp/schedule', data=json.dumps(data), headers=headers)
        retval = dict(r.json())
        print(retval)

        await asyncio.sleep(4)

        assert 200 == r.status_code
        assert not retval['schedule']['id'] == None
        assert retval['schedule']['exclusive'] == True
        assert retval['schedule']['type'] == "INTERVAL"
        assert retval['schedule']['time'] == "None"
        assert retval['schedule']['day'] == None
        assert retval['schedule']['process_name'] == 'sleep30'
        assert retval['schedule']['repeat'] == '0:00:45'
        assert retval['schedule']['name'] == 'test_post_sch'

        # TODO: Move this code block to a proper teardown method
        await remove_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "stop"])
        await asyncio.sleep(4)


    @pytest.mark.asyncio
    async def test_update_schedule(self):
        # TODO: Move this code block to a proper setup method
        await add_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "start"])
        await asyncio.sleep(4)

        # First create a schedule to get the schedule_id
        headers = {"Content-Type": 'application/json'}
        data = {"type": 3, "name": "test_update_sch", "process_name": "sleep30", "repeat": "45"}

        r = requests.post('http://localhost:8082/foglamp/schedule', data=json.dumps(data), headers=headers)
        retval = dict(r.json())

        await asyncio.sleep(4)

        assert 200 == r.status_code
        schedule_id = retval['schedule']['id']

        # Secondly, update the schedule
        headers = {"Content-Type": 'application/json'}
        data = {"name": "test_update_sch_upd", "repeat": "4"}

        r = requests.put('http://localhost:8082/foglamp/schedule/'+schedule_id, data=json.dumps(data), headers=headers)
        retval = dict(r.json())

        await asyncio.sleep(4)

        assert 200 == r.status_code
        assert not retval['schedule']['id'] == None

        # These values did not change
        assert retval['schedule']['exclusive'] == True
        # TODO: There is a bug in core/scheduler.py. It does not update the schedule type BUT if you pass a new schedule
        # type in above, it will return the new schedule type even though it does not update the DB record.
        assert retval['schedule']['type'] == "INTERVAL"
        assert retval['schedule']['time'] == "None"
        assert retval['schedule']['day'] == None
        assert retval['schedule']['process_name'] == 'sleep30'

        # Below two values only changed
        assert retval['schedule']['repeat'] == '0:00:04'
        assert retval['schedule']['name'] == 'test_update_sch_upd'

        # TODO: Move this code block to a proper teardown method
        await remove_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "stop"])
        await asyncio.sleep(4)


    @pytest.mark.asyncio
    async def test_delete_schedule(self):
        # TODO: Move this code block to a proper setup method
        await add_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "start"])
        await asyncio.sleep(4)

        # First create a schedule to get the schedule_id
        headers = {"Content-Type": 'application/json'}
        data = {"type": 3, "name": "test_delete_sch", "process_name": "sleep30", "repeat": "45"}

        r = requests.post('http://localhost:8082/foglamp/schedule', data=json.dumps(data), headers=headers)
        retval = dict(r.json())

        await asyncio.sleep(4)

        assert 200 == r.status_code
        schedule_id = retval['schedule']['id']

        # Now check the schedules
        r = requests.delete('http://localhost:8082/foglamp/schedule/' + schedule_id)
        retval = dict(r.json())

        assert 200 == r.status_code
        assert retval['id'] == schedule_id
        assert retval['message'] == "Schedule deleted successfully"

        # TODO: Move this code block to a proper teardown method
        await remove_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "stop"])
        await asyncio.sleep(4)


    @pytest.mark.asyncio
    async def test_get_schedule(self):
        # TODO: Move this code block to a proper setup method
        await add_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "start"])
        await asyncio.sleep(4)

        # First create a schedule to get the schedule_id
        headers = {"Content-Type": 'application/json'}
        data = {"type": 3, "name": "test_get_sch", "process_name": "sleep30", "repeat": "45"}

        r = requests.post('http://localhost:8082/foglamp/schedule', data=json.dumps(data), headers=headers)
        retval = dict(r.json())

        await asyncio.sleep(4)

        assert 200 == r.status_code
        schedule_id = retval['schedule']['id']

        # Now check the schedule
        r = requests.get('http://localhost:8082/foglamp/schedule/' + schedule_id)
        retval = dict(r.json())

        assert 200 == r.status_code
        assert retval['id'] == schedule_id
        assert retval['exclusive'] == True
        assert retval['type'] == "INTERVAL"
        assert retval['time'] == "None"
        assert retval['day'] == None
        assert retval['process_name'] == 'sleep30'
        assert retval['repeat'] == '0:00:45'
        assert retval['name'] == 'test_get_sch'

        # TODO: Move this code block to a proper teardown method
        await remove_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "stop"])
        await asyncio.sleep(4)


    @pytest.mark.asyncio
    async def test_get_schedules(self):
        # TODO: Move this code block to a proper setup method
        await add_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "start"])
        await asyncio.sleep(4)

        # First create two schedules to get the schedule_id
        headers = {"Content-Type": 'application/json'}
        data = {"type": 3, "name": "test_get_schA", "process_name": "sleep30", "repeat": "45"}

        r = requests.post('http://localhost:8082/foglamp/schedule', data=json.dumps(data), headers=headers)
        retval = dict(r.json())

        assert 200 == r.status_code
        schedule_id1 = retval['schedule']['id']

        headers = {"Content-Type": 'application/json'}
        data = {"type": 2, "name": "test_get_schB", "process_name": "sleep30", "day": 5, "time": 44500}

        r = requests.post('http://localhost:8082/foglamp/schedule', data=json.dumps(data), headers=headers)
        retval = dict(r.json())

        await asyncio.sleep(4)

        assert 200 == r.status_code
        schedule_id2 = retval['schedule']['id']

        # Now check the schedules
        r = requests.get('http://localhost:8082/foglamp/schedule')
        retval = dict(r.json())

        assert 200 == r.status_code
        assert 2 == len(retval['schedules'])

        # Because of unpredictibility in the sequence of the items, this method of assert has been adopted
        assert retval['schedules'][0]['id'] in [schedule_id1, schedule_id2]
        assert retval['schedules'][0]['exclusive'] == True
        assert retval['schedules'][0]['type'] in ["INTERVAL", "TIMED"]
        assert retval['schedules'][0]['time'] in ["None", '12:21:40']
        assert retval['schedules'][0]['day'] in [None, 5]
        assert retval['schedules'][0]['process_name'] == 'sleep30'
        assert retval['schedules'][0]['repeat'] in ['0:00:45', '0:00:00']
        assert retval['schedules'][0]['name'] in ['test_get_schA', 'test_get_schB']

        assert retval['schedules'][1]['id'] in [schedule_id1, schedule_id2]
        assert retval['schedules'][1]['exclusive'] == True
        assert retval['schedules'][1]['type'] in ["INTERVAL", "TIMED"]
        assert retval['schedules'][1]['time'] in ["None", '12:21:40']
        assert retval['schedules'][1]['day'] in [None, 5]
        assert retval['schedules'][1]['process_name'] == 'sleep30'
        assert retval['schedules'][1]['repeat'] in ['0:00:45', '0:00:00']
        assert retval['schedules'][1]['name'] in ['test_get_schA', 'test_get_schB']

        # TODO: Move this code block to a proper teardown method
        await remove_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "stop"])
        await asyncio.sleep(4)


    @pytest.mark.asyncio
    async def test_start_schedule(self):
        # TODO: Move this code block to a proper setup method
        await add_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "start"])
        await asyncio.sleep(4)

        # First create a schedule to get the schedule_id
        headers = {"Content-Type": 'application/json'}
        data = {"type": 3, "name": "test_start_sch", "process_name": "sleep30", "repeat": "600"}

        r = requests.post('http://localhost:8082/foglamp/schedule', data=json.dumps(data), headers=headers)
        retval = dict(r.json())

        assert 200 == r.status_code
        schedule_id = retval['schedule']['id']

        # Now start the schedules
        r = requests.post('http://localhost:8082/foglamp/schedule/start/' + schedule_id)
        retval = dict(r.json())

        await asyncio.sleep(4)

        assert 200 == r.status_code
        assert retval['id'] == schedule_id
        assert retval['message'] == "Schedule started successfully"

        # Verify with Task record as to one task has been created
        r = requests.get('http://localhost:8082/foglamp/task')
        retval = dict(r.json())

        assert 200 == r.status_code
        assert 1 == len(retval['tasks'])
        assert retval['tasks'][0]['state'] == 'RUNNING'
        assert retval['tasks'][0]['process_name'] == 'sleep30'

        # TODO: Move this code block to a proper teardown method
        # Cancel running tasks, if any
        r = requests.get('http://localhost:8082/foglamp/task')
        retval = dict(r.json())

        running_tasks = retval['tasks']
        for task in running_tasks:
            task_id = task['id']
            # Now cancel the runnung task
            r = requests.put('http://localhost:8082/foglamp/task/cancel/' + task_id)
            retval = dict(r.json())

            assert 200 == r.status_code
            assert retval['id'] == task_id
            assert retval['message'] == "Task cancelled successfully"

            await asyncio.sleep(4)

            # Verify the task has been cancelled
            r = requests.get('http://localhost:8082/foglamp/task/' + task_id)
            retval = dict(r.json())

            assert 200 == r.status_code
            assert retval['id'] == task_id
            assert retval['state'] == 'CANCELED'

        await remove_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "stop"])
        await asyncio.sleep(4)


    @pytest.mark.asyncio
    async def test_get_task(self):
        # TODO: Move this code block to a proper setup method
        await add_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "start"])
        await asyncio.sleep(4)

        # First create a schedule to get the schedule_id
        headers = {"Content-Type": 'application/json'}
        data = {"type": 4, "name": "test_get_task1", "process_name": "sleep10"}

        r = requests.post('http://localhost:8082/foglamp/schedule', data=json.dumps(data), headers=headers)
        retval = dict(r.json())

        assert 200 == r.status_code
        schedule_id = retval['schedule']['id']

        # Now start the schedule to create a Task
        r = requests.post('http://localhost:8082/foglamp/schedule/start/' + schedule_id)
        retval = dict(r.json())

        assert 200 == r.status_code
        assert retval['id'] == schedule_id
        assert retval['message'] == "Schedule started successfully"

        await asyncio.sleep(4)

        # Verify with Task record as to one task has been created
        r = requests.get('http://localhost:8082/foglamp/task')
        retval = dict(r.json())

        task_id = retval['tasks'][0]['id']

        assert 200 == r.status_code

        # Get Task
        r = requests.get('http://localhost:8082/foglamp/task/' + task_id)
        retval = dict(r.json())

        assert 200 == r.status_code
        assert retval['id'] == task_id

        # TODO: Move this code block to a proper teardown method
        # Cancel running tasks, if any
        r = requests.get('http://localhost:8082/foglamp/task')
        retval = dict(r.json())

        running_tasks = retval['tasks']
        for task in running_tasks:
            task_id = task['id']
            # Now cancel the runnung task
            r = requests.put('http://localhost:8082/foglamp/task/cancel/' + task_id)
            retval = dict(r.json())

            assert 200 == r.status_code
            assert retval['id'] == task_id
            assert retval['message'] == "Task cancelled successfully"

            await asyncio.sleep(4)

            # Verify the task has been cancelled
            r = requests.get('http://localhost:8082/foglamp/task/' + task_id)
            retval = dict(r.json())

            assert 200 == r.status_code
            assert retval['id'] == task_id
            assert retval['state'] == 'CANCELED'

        await remove_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "stop"])
        await asyncio.sleep(4)

    @pytest.mark.asyncio
    async def test_get_tasks(self):
        # TODO: Move this code block to a proper setup method
        await add_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "start"])
        await asyncio.sleep(4)

        # First create a schedule to get the schedule_id
        headers = {"Content-Type": 'application/json'}
        data = {"type": 4, "name": "test_get_task1", "process_name": "sleep10"}

        r = requests.post('http://localhost:8082/foglamp/schedule', data=json.dumps(data), headers=headers)
        retval = dict(r.json())

        assert 200 == r.status_code
        schedule_id = retval['schedule']['id']

        # Now start the schedule to create a Task
        r = requests.post('http://localhost:8082/foglamp/schedule/start/' + schedule_id)
        retval = dict(r.json())

        assert 200 == r.status_code
        assert retval['id'] == schedule_id
        assert retval['message'] == "Schedule started successfully"

        await asyncio.sleep(4)

        # Now start the schedule again to create another Task
        r = requests.post('http://localhost:8082/foglamp/schedule/start/' + schedule_id)
        retval = dict(r.json())

        assert 200 == r.status_code
        assert retval['id'] == schedule_id
        assert retval['message'] == "Schedule started successfully"

        await asyncio.sleep(4)

        # Verify with Task record as to two  tasks have been created
        rr = requests.get('http://localhost:8082/foglamp/task')
        retvall = dict(rr.json())
        print(retvall)

        assert 200 == rr.status_code
        assert 2 == len(retvall['tasks'])
        assert retvall['tasks'][0]['process_name'] == 'sleep10'
        assert retvall['tasks'][1]['process_name'] == 'sleep10'

        # TODO: Move this code block to a proper teardown method
        # Cancel running tasks, if any
        r = requests.get('http://localhost:8082/foglamp/task')
        retval = dict(r.json())

        running_tasks = retval['tasks']
        for task in running_tasks:
            task_id = task['id']
            # Now cancel the runnung task
            r = requests.put('http://localhost:8082/foglamp/task/cancel/' + task_id)
            retval = dict(r.json())

            assert 200 == r.status_code
            assert retval['id'] == task_id
            assert retval['message'] == "Task cancelled successfully"

            await asyncio.sleep(4)

            # Verify the task has been cancelled
            r = requests.get('http://localhost:8082/foglamp/task/' + task_id)
            retval = dict(r.json())

            assert 200 == r.status_code
            assert retval['id'] == task_id
            assert retval['state'] == 'CANCELED'

        await remove_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "stop"])
        await asyncio.sleep(4)


    @pytest.mark.asyncio
    async def test_get_tasks_latest(self):
        # TODO: Move this code block to a proper setup method
        await add_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "start"])
        await asyncio.sleep(4)

        # First create a schedule to get the schedule_id
        headers = {"Content-Type": 'application/json'}
        data = {"type": 4, "name": "test_get_task1", "process_name": "sleep10"}

        r = requests.post('http://localhost:8082/foglamp/schedule', data=json.dumps(data), headers=headers)
        retval = dict(r.json())

        assert 200 == r.status_code
        schedule_id = retval['schedule']['id']

        # Now start the schedule to create a Task
        r = requests.post('http://localhost:8082/foglamp/schedule/start/' + schedule_id)
        retval = dict(r.json())

        await asyncio.sleep(4)

        assert 200 == r.status_code
        assert retval['id'] == schedule_id
        assert retval['message'] == "Schedule started successfully"

        # Now start the schedule again to create another Task
        r = requests.post('http://localhost:8082/foglamp/schedule/start/' + schedule_id)
        retval = dict(r.json())

        assert 200 == r.status_code
        assert retval['id'] == schedule_id
        assert retval['message'] == "Schedule started successfully"

        await asyncio.sleep(4)

        # Verify with Task record as to two tasks have been created
        r = requests.get('http://localhost:8082/foglamp/task')
        retval = dict(r.json())

        assert 200 == r.status_code
        assert 2 == len(retval['tasks'])

        # Verify only one Task record is returned
        r = requests.get('http://localhost:8082/foglamp/task/latest')
        retval = dict(r.json())

        assert 200 == r.status_code
        assert 1 == len(retval['tasks'])
        assert retval['tasks'][0]['process_name'] == 'sleep10'

        # TODO: Move this code block to a proper teardown method
        # Cancel running tasks, if any
        r = requests.get('http://localhost:8082/foglamp/task')
        retval = dict(r.json())

        running_tasks = retval['tasks']
        for task in running_tasks:
            task_id = task['id']
            # Now cancel the runnung task
            r = requests.put('http://localhost:8082/foglamp/task/cancel/' + task_id)
            retval = dict(r.json())

            assert 200 == r.status_code
            assert retval['id'] == task_id
            assert retval['message'] == "Task cancelled successfully"

            await asyncio.sleep(4)

            # Verify the task has been cancelled
            r = requests.get('http://localhost:8082/foglamp/task/' + task_id)
            retval = dict(r.json())

            assert 200 == r.status_code
            assert retval['id'] == task_id
            assert retval['state'] == 'CANCELED'

        await remove_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "stop"])
        await asyncio.sleep(4)


    @pytest.mark.asyncio
    async def test_cancel_task(self):
        # TODO: Move this code block to a proper setup method
        await add_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "start"])
        await asyncio.sleep(4)

        # First create a schedule to get the schedule_id
        headers = {"Content-Type": 'application/json'}
        data = {"type": 3, "name": "test_start_sch", "process_name": "sleep30", "repeat": "600"}

        r = requests.post('http://localhost:8082/foglamp/schedule', data=json.dumps(data), headers=headers)
        retval = dict(r.json())

        assert 200 == r.status_code
        schedule_id = retval['schedule']['id']

        # Now start the schedules
        r = requests.post('http://localhost:8082/foglamp/schedule/start/' + schedule_id)
        retval = dict(r.json())

        assert 200 == r.status_code
        assert retval['id'] == schedule_id
        assert retval['message'] == "Schedule started successfully"

        await asyncio.sleep(4)

        # Verify with Task record as to one task has been created
        r = requests.get('http://localhost:8082/foglamp/task')
        retval = dict(r.json())
        task_id = retval['tasks'][0]['id']

        assert 200 == r.status_code
        assert 1 == len(retval['tasks'])
        assert retval['tasks'][0]['state'] == 'RUNNING'
        assert retval['tasks'][0]['process_name'] == 'sleep30'

        # Now cancel the runnung task
        r = requests.put('http://localhost:8082/foglamp/task/cancel/' + task_id)
        retval = dict(r.json())

        assert 200 == r.status_code
        assert retval['id'] == task_id
        assert retval['message'] == "Task cancelled successfully"

        await asyncio.sleep(4)

        # Verify the task has been cancelled
        r = requests.get('http://localhost:8082/foglamp/task/' + task_id)
        retval = dict(r.json())

        assert 200 == r.status_code
        assert retval['id'] == task_id
        assert retval['state'] == 'CANCELED'

        # TODO: Move this code block to a proper teardown method
        await remove_test_data()
        await asyncio.sleep(4)
        from subprocess import call
        call(["foglamp", "stop"])
        await asyncio.sleep(4)
