#TODO 需要一个属性描述符，方便调用任务的各种属性

#TODO 需要一个对所有输入的汇总，并用这些信息发起任务

import yaml


def load_config(fn='./task.yml'):
    try:
        with open(fn, 'r') as fin:
            return yaml.load(fin)
    except FileNotFoundError as e:
        return {}
