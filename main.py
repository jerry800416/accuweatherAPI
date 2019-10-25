# -*- coding: utf-8 -*-
from datetime import datetime,timedelta
from lib import connect_DB,get_api_key,acc_weather_api,return_key_status,go_to_log
import ref

#######################################
#待新增修改：
#補傳資料功能
# try except log
# 若新增節點,沒有這個資料庫的話,新增此資料庫
# 補齊註解
#######################################

now = datetime.now()+timedelta(hours=1)
# 獲取有哪些塔需要拉取天氣資訊
segids = connect_DB(ref.db_info,'Gridwell',ref.sql,'select',0)
segids_tb = connect_DB(ref.db_info,'TowerBase_Gridwell',ref.sql,'select',0)
# 獲取目前可用的api_key
api_key = get_api_key(ref.db_info,'Gridwell')

# 拉取天氣資訊並push 上DB(架空)
for segid in segids:
    api_key = acc_weather_api(now,segid[0],api_key,ref.log_path,'Gridwell')

# 拉取天氣資訊並push 上DB(塔基)
for segid_tb in segids_tb:
    api_key = acc_weather_api(now,segid_tb[0],api_key,ref.log_path,'TowerBase_Gridwell')

# 每天？點的時候重置api_key的status
return_key_status(ref.db_info,'Gridwell',4)

go_to_log(ref.log_path,'all data complete')