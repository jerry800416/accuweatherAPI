# -*- coding: utf-8 -*-

# db_connect
db_info = ('211.23.162.130','gridwell','gridwell123',3306)
# db_info = ('localhost','gridwell','gridwell123',3306)
db_info2 = ('140.112.94.59','gridwell','gridwell123',30303)
select_db_name = "Gridwell"
insert_db_name = "DTR_realtime_acc"
pre_db_name = "DTR_forcast_acc"
sql = "SELECT DISTINCT SegID FROM Relation WHERE node_status = 1"
log_path = './acc.log'
Tc = 80  # 線溫上限
