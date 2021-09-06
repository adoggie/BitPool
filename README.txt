
内存使用计算
==================

60*24*365 * 64 / (1024*1024.)  * 126 * 2 =
2years , 126 symbols = 8084M = 8G

= 32G / one symbol per year


shared memory inspect:
---------------------
 ipcs -m --human | grep f0
	

bitpool (python 2.x ) 
================
	
python -m elabs.service.bitpool.BitPool 

python -m elabs.service.bitpool.BitPool get_latest 'AXSUSDT' 40 			读取最近一根40分钟k线
python -m elabs.service.bitpool.BitPool get_latest 'AXSUSDT' 40  5 			读取最近5根40分钟k线
python -m elabs.service.bitpool.BitPool get_data 'AXSUSDT' 40 '2021-9-2' '2021-9-3'
python -m elabs.service.bitpool.BitPool create 		创建共享区，内存清除，建立索引
python -m elabs.service.bitpool.BitPool destroy 	摧毁共享区
python -m elabs.service.bitpool.BitPool mem_zero	内存清除
python -m elabs.service.bitpool.BitPool edit 		编辑配置
python -m elabs.service.bitpool.BitPool sync 		同步行情 默认 5分钟


python -m elabs.service.bitpool.csv_to_mem load_csv_all '2021-1-1' '2021-3-1'   	导入所有1M ，并进行合成周期K线
python -m elabs.service.bitpool.csv_to_mem load_csv   'AXSUSDT' '2021-1-1' '2021-3-1'	导入指定品种 1M ，并进行合成周期K线


API
-------
See elabs/service/bitpool/tests/client.py

from elabs.service.bitpool.BitPool import client
client().get_data('AXSUSDT',40,'2021-9-2','2021-9-3')
client().get_latest('AXSUSDT',40)


Cron Task
--------
*/1 * * * * PYTHONPATH=/home/eladmin/projects/bitpool-xianliang-20210902 /home/eladmin/anaconda2/bin/python sync

watch -n 2 -d "python -m elabs.service.bitpool.BitPool get_latest 'AXSUSDT' 40 5"

