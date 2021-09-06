#coding:utf-8

import elabs.service.bitpool.config
import time
import fire

from elabs.service.bitpool.BitPool import client


def simple():
    return client().get_data('AXSUSDT',40,'2021-9-2','2021-9-3')
    # return client().get_latest('AXSUSDT',40)

if __name__ == '__main__':
    fire.Fire()