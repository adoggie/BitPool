#coding:utf-8

"""
https://gist.github.com/minrk/4667957
"""

import os
import string
import sys
import time
from random import randint
import zmq


import config

xpub_url = config.MX_PUB_ADDR
xsub_url = config.MX_SUB_ADDR

ctx = zmq.Context()

def broker():
    xpub = ctx.socket(zmq.XPUB)
    xpub.bind(xpub_url)
    xsub = ctx.socket(zmq.XSUB)
    xsub.bind(xsub_url)
    xsub.send(b'\x01')

    poller = zmq.Poller()
    # poller.register(xpub, zmq.POLLIN)
    poller.register(xsub, zmq.POLLIN)
    while True:
        events = dict(poller.poll(1000))
        # if xpub in events:
        #     message = xsub.recv_string()
        #     print message
        #     xsub.send_string(message)
        if xsub in events:
            message = xsub.recv_string()
            xpub.send_string(message)

if __name__ == '__main__':
    broker()