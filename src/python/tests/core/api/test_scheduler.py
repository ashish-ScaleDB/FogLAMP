# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

import json
import requests
import time


__author__ = "Amarendra K Sinha"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

# Module attributes

BASE_URL = "http://localhost:8082/foglamp"
headers = {'Content-Type': 'application/json'}


class TestScheduler(object):

    # TODO: Add tests for negative cases.
    # run python -m foglamp.core in foreground
    # no need to have test data, create schedule, start etc. at run time

    def _create_schedule(self, data):
        response = requests.post(BASE_URL + '/schedule', data=json.dumps(data), headers=headers)
        return response

    def _start_schedule(self, schedule_id):
        response = requests.post(BASE_URL + '/schedule/start/' + schedule_id)
        return dict(response.json())

    def _delete_schedule(self, schedule_id):
        response = requests.delete(BASE_URL + '/schedule/' + schedule_id)
        return dict(response.json())

    def _verify_schedule(self, expected, actual):
        assert expected['exclusive'] == actual['exclusive']
        assert expected['type'] == actual['type']
        assert expected['time'] == actual['time']
        assert expected['day'] == actual['day']
        assert expected['process_name'] == actual['process_name']
        assert expected['repeat'] == actual['repeat']
        assert expected['name'] == actual['name']

    def test_get_scheduled_processes(self):
        # setup: \i scheduler_api_demo_data.sql
        response = requests.get(BASE_URL + '/schedule/process')
        actual = dict(response.json())

        assert 200 == response.status_code

        processes = actual['processes']
        assert 2 == len(processes)
        assert 'sleep1' in processes
        assert 'sleep10' in processes

    def test_get_scheduled_process(self):
        response = requests.get(BASE_URL + '/schedule/process/sleep1')
        assert 200 == response.status_code
        assert 'sleep1' == response.json()

    def test_post_schedule(self):
        d = {"type": 3, "name": "test_post_sch", "process_name": "sleep10", "repeat": "3600"}
        response = self._create_schedule(data=d)
        assert 200 == response.status_code

        retval = dict(response.json())
        schedule_id = retval['schedule']['id']
        assert schedule_id is not None
        expected = {"exclusive": True, "type": "INTERVAL", "time": "None", "day": None,
                    "process_name": "sleep10", "repeat":"1:00:00", "name": "test_post_sch"}
        self._verify_schedule(expected, retval['schedule'])

        # cleanup
        self._delete_schedule(schedule_id)

    def test_update_schedule(self):
        # setup: First create a schedule to get the schedule_id
        d = {"type": 3, "name": "test_sch", "process_name": "sleep10", "repeat": "3600"}
        response = self._create_schedule(data=d)
        sch_json = dict(response.json())

        # update the schedule
        data = {"name": "test_update_sch_upd", "repeat": "4"}

        r = requests.put(BASE_URL+'/schedule/' + sch_json['schedule']['id'], data=json.dumps(data), headers=headers)
        retval = dict(r.json())
        schedule_id = sch_json['schedule']['id']

        assert 200 == r.status_code
        assert schedule_id is not None

        # TODO: There is a bug in core/scheduler.py. It does not update the schedule type BUT if you pass a new schedule
        # type in above, it will return the new schedule type even though it does not update the DB record.
        # only repeat and name will be changed
        expected = {"exclusive": True, "type": "INTERVAL", "time": "None", "day": None,
                    "process_name": "sleep10", "repeat": "0:00:04", "name": "test_update_sch_upd"}
        self._verify_schedule(expected, retval['schedule'])

        # cleanup
        self._delete_schedule(schedule_id)

    def test_delete_schedule(self):
        # set up: First create a schedule to get the schedule_id
        d = {"type": 3, "name": "test_sch", "process_name": "sleep10", "repeat": "3600"}
        response = self._create_schedule(data=d)
        sch_json = dict(response.json())
        schedule_id = sch_json['schedule']['id']

        # Now check the schedules
        r = requests.delete(BASE_URL + '/schedule/' + schedule_id)
        retval = dict(r.json())

        assert 200 == r.status_code
        assert retval['id'] == schedule_id
        assert retval['message'] == "Schedule deleted successfully"

    def test_get_schedule(self):
        # set up: First create a schedule to get the schedule_id
        d = {"type": 3, "name": "test_get_sch", "process_name": "sleep10", "repeat": "3600"}
        response = self._create_schedule(data=d)
        sch_json = dict(response.json())
        schedule_id = sch_json['schedule']['id']

        # Now check the schedule
        r = requests.get(BASE_URL + '/schedule/' + schedule_id)
        retval = dict(r.json())

        assert 200 == r.status_code
        assert retval['id'] == schedule_id

        expected = {"exclusive": True, "type": "INTERVAL", "time": "None", "day": None,
                    "process_name": "sleep10", "repeat": "1:00:00", "name": "test_get_sch"}
        self._verify_schedule(expected, retval)

        # cleanup
        self._delete_schedule(schedule_id)

    def test_get_schedules(self):
        # setup: First create two schedules to get the schedule_id
        d1 = {"type": 3, "name": "test_get_schA", "process_name": "sleep10", "repeat": "3600"}
        response1 = self._create_schedule(data=d1)
        sch_json1 = dict(response1.json())
        schedule_id1 = sch_json1['schedule']['id']

        d2 = {"type": 2, "name": "test_get_schB", "process_name": "sleep10", "day": 5, "time": 44500}
        response2 = self._create_schedule(data=d2)
        sch_json2 = dict(response2.json())
        schedule_id2 = sch_json2['schedule']['id']

        time.sleep(2)

        # Now check the schedules
        r = requests.get(BASE_URL + '/schedule')
        retval = dict(r.json())

        assert 200 == r.status_code
        assert 2 == len(retval['schedules'])

        # Because of unpredictibility in the sequence of the items, this method of assert has been adopted
        assert retval['schedules'][0]['id'] in [schedule_id1, schedule_id2]
        assert retval['schedules'][0]['exclusive'] is True
        assert retval['schedules'][0]['type'] in ["INTERVAL", "TIMED"]
        assert retval['schedules'][0]['time'] in ["None", '12:21:40']
        assert retval['schedules'][0]['day'] in [None, 5]
        assert retval['schedules'][0]['process_name'] == 'sleep10'
        assert retval['schedules'][0]['repeat'] in ['1:00:00', '0:00:00']
        assert retval['schedules'][0]['name'] in ['test_get_schA', 'test_get_schB']

        assert retval['schedules'][1]['id'] in [schedule_id1, schedule_id2]
        assert retval['schedules'][1]['exclusive'] is True
        assert retval['schedules'][1]['type'] in ["INTERVAL", "TIMED"]
        assert retval['schedules'][1]['time'] in ["None", '12:21:40']
        assert retval['schedules'][1]['day'] in [None, 5]
        assert retval['schedules'][1]['process_name'] == 'sleep10'
        assert retval['schedules'][1]['repeat'] in ['1:00:00', '0:00:00']
        assert retval['schedules'][1]['name'] in ['test_get_schA', 'test_get_schB']

        # clean up data
        self._delete_schedule(schedule_id1)
        self._delete_schedule(schedule_id2)

    def test_start_schedule(self):
        # setup: First create a schedule to get the schedule_id
        d = {"type": 3, "name": "test_start_sch", "process_name": "sleep10", "repeat": "10"}
        response = self._create_schedule(data=d)
        sch_json = dict(response.json())
        schedule_id = sch_json['schedule']['id']

        # Now start the schedules
        r = requests.post(BASE_URL + '/schedule/start/' + schedule_id)
        retval = dict(r.json())

        assert 200 == r.status_code
        assert retval['id'] == schedule_id
        assert retval['message'] == "Schedule started successfully"

        # Allow sufficient time for task record to be created
        time.sleep(3)

        # Verify with Task record as to one task has been created
        r = requests.get(BASE_URL + '/task')
        retval = dict(r.json())

        assert 200 == r.status_code
        assert 1 == len(retval['tasks'])
        assert retval['tasks'][0]['state'] == 'RUNNING'
        assert retval['tasks'][0]['process_name'] == 'sleep10'

        # clean up data
        self._delete_schedule(schedule_id)

    # def test_get_task(self):
    #     # First create a schedule to get the schedule_id
    #     data = {"type": 4, "name": "test_get_task1", "process_name": "sleep10"}
    #
    #     r = requests.post(BASE_URL+'/schedule', data=json.dumps(data), headers=headers)
    #     retval = dict(r.json())
    #
    #     assert 200 == r.status_code
    #     schedule_id = retval['schedule']['id']
    #
    #     # Now start the schedule to create a Task
    #     r = requests.post(BASE_URL+'/schedule/start/' + schedule_id)
    #     retval = dict(r.json())
    #
    #     assert 200 == r.status_code
    #     assert retval['id'] == schedule_id
    #     assert retval['message'] == "Schedule started successfully"
    #
    #     # Allow sufficient time for task record to be created
    #     asyncio.sleep(4)
    #
    #     # Verify with Task record as to one task has been created
    #     r = requests.get(BASE_URL+'/task')
    #     retval = dict(r.json())
    #
    #     task_id = retval['tasks'][0]['id']
    #
    #     assert 200 == r.status_code
    #
    #     # Get Task
    #     r = requests.get(BASE_URL+'/task/' + task_id)
    #     retval = dict(r.json())
    #
    #     assert 200 == r.status_code
    #     assert retval['id'] == task_id
    #
    # def test_get_tasks(self):
    #     # First create a schedule to get the schedule_id
    #     data = {"type": 3, "name": "test_get_task1", "process_name": "sleep1", "repeat": 2}
    #
    #     r = requests.post(BASE_URL+'/schedule', data=json.dumps(data), headers=headers)
    #     retval = dict(r.json())
    #
    #     assert 200 == r.status_code
    #     schedule_id = retval['schedule']['id']
    #
    #     # Now start the schedule to create a Task
    #     r = requests.post(BASE_URL+'/schedule/start/' + schedule_id)
    #     retval = dict(r.json())
    #
    #     assert 200 == r.status_code
    #     assert retval['id'] == schedule_id
    #     assert retval['message'] == "Schedule started successfully"
    #
    #     # Allow multiple task records to be created
    #     asyncio.sleep(10)
    #
    #     # Verify with Task record as to two  tasks have been created
    #     rr = requests.get(BASE_URL+'/task')
    #     retvall = dict(rr.json())
    #
    #     assert 200 == rr.status_code
    #     assert len(retvall['tasks']) > 0
    #     assert retvall['tasks'][0]['process_name'] == 'sleep1'
    #     assert retvall['tasks'][1]['process_name'] == 'sleep1'
    #
    # def test_get_tasks_latest(self):
    #     # First create a schedule to get the schedule_id
    #     data = {"type": 3, "name": "test_get_task1", "process_name": "sleep1", "repeat": 2}
    #
    #     r = requests.post(BASE_URL+'/schedule', data=json.dumps(data), headers=headers)
    #     retval = dict(r.json())
    #
    #     assert 200 == r.status_code
    #     schedule_id = retval['schedule']['id']
    #
    #     asyncio.sleep(4)
    #
    #     # Now start the schedule to create a Task
    #     r = requests.post(BASE_URL+'/schedule/start/' + schedule_id)
    #     retval = dict(r.json())
    #
    #     assert 200 == r.status_code
    #     assert retval['id'] == schedule_id
    #     assert retval['message'] == "Schedule started successfully"
    #
    #     # Allow multiple tasks to be created
    #     asyncio.sleep(10)
    #
    #     # Verify with Task record as to more than one task have been created
    #     r = requests.get(BASE_URL+'/task')
    #     retval = dict(r.json())
    #
    #     assert 200 == r.status_code
    #     assert len(retval['tasks']) > 0
    #
    #     # Verify only one Task record is returned
    #     r = requests.get(BASE_URL+'/task/latest')
    #     retval = dict(r.json())
    #
    #     assert 200 == r.status_code
    #     assert 1 == len(retval['tasks'])
    #     assert retval['tasks'][0]['process_name'] == 'sleep1'
    #
    #
    # def test_cancel_task(self):
    #     # First create a schedule to get the schedule_id
    #     data = {"type": 3, "name": "test_start_sch", "process_name": "sleep30", "repeat": "3600"}
    #
    #     r = requests.post(BASE_URL+'/schedule', data=json.dumps(data), headers=headers)
    #     retval = dict(r.json())
    #
    #     assert 200 == r.status_code
    #     schedule_id = retval['schedule']['id']
    #
    #     # Now start the schedules
    #     r = requests.post(BASE_URL+'/schedule/start/' + schedule_id)
    #     retval = dict(r.json())
    #
    #     assert 200 == r.status_code
    #     assert retval['id'] == schedule_id
    #     assert retval['message'] == "Schedule started successfully"
    #
    #     # Allow sufficient time for task record to be created
    #     asyncio.sleep(4)
    #
    #     # Verify with Task record as to one task has been created
    #     r = requests.get(BASE_URL+'/task')
    #     retval = dict(r.json())
    #     task_id = retval['tasks'][0]['id']
    #
    #     assert 200 == r.status_code
    #     assert 1 == len(retval['tasks'])
    #     assert retval['tasks'][0]['state'] == 'RUNNING'
    #     assert retval['tasks'][0]['process_name'] == 'sleep30'
    #
    #     # Now cancel the runnung task
    #     r = requests.put(BASE_URL+'/task/cancel/' + task_id)
    #     retval = dict(r.json())
    #
    #     assert 200 == r.status_code
    #     assert retval['id'] == task_id
    #     assert retval['message'] == "Task cancelled successfully"
    #
    #     # Allow sufficient time for task record to be created
    #     asyncio.sleep(4)
    #
    #     # Verify the task has been cancelled
    #     r = requests.get(BASE_URL+'/task/' + task_id)
    #     retval = dict(r.json())
    #
    #     assert 200 == r.status_code
    #     assert retval['id'] == task_id
    #     assert retval['state'] == 'CANCELED'
