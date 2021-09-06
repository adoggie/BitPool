#!/usr/bin/env bash

pwd=$(cd `dirname $0`;pwd)
cd $pwd

/home/eladmin/anaconda2/bin/python -m elabs.service.bitpool.BitPool sync