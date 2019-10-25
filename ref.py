# -*- coding: utf-8 -*-

# db_connect
db_info = ()
db_info2 = ()
select_db_name = "Gridwell"
insert_db_name = "DTR_realtime_acc"
pre_db_name = "DTR_forcast_acc"
sql = "SELECT DISTINCT SegID FROM Relation WHERE node_status = 1"
log_path = './acc.log'
Tc = 80  # 線溫上限
