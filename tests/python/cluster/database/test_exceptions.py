from dxl.cluster.database.exceptions import *
from .data_for_test import newdata

id = newdata['id']


def test_TaskNotFoundError():
    task = TaskNotFoundError(id)
    assert repr(task) == """TaskNotFoundError('Task with id: {id} not found.',)""".format(id=id)


def test_InvalidJSONForTask():
    for key in newdata.keys():
        try:
            raise InvalidJSONForTask(
                "Required key: {key} is not found in JSON string: {newdata}".format(key=key, newdata=newdata))
        except Exception as e:
            assert isinstance(e, InvalidJSONForTask)
            assert str(e) == "Required key: {key} is not found in JSON string: {newdata}".format(key=key,
                                                                                                 newdata=newdata)


def test_InvalidJSONForTask2():
    for key, value in newdata.items():
        try:
            raise InvalidJSONForTask(
                "Wrong type for key: {key} with value: {value}".format(key=key, value=value))
        except Exception as e:
            assert isinstance(e, InvalidJSONForTask)
            assert str(e) == """Wrong type for key: {key} with value: {value}""".format(key=key, value=value)


def test_InvalidJSONForTask3():
    try:
        raise InvalidJSONForTask("'__task__' not found or is False for JSON: {}".format(newdata))
    except Exception as e:
        assert isinstance(e, InvalidJSONForTask)
        assert str(e) == "'__task__' not found or is False for JSON: {}".format(newdata)
