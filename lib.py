# -*- coding: utf-8 -*-
import MySQLdb
from geographiclib.geodesic import Geodesic
from datetime import datetime,timedelta
from DTR_161 import Solve_I,PHI
import time
import ref
import os
import math
import requests
import json


class accuweather_api():
    '''
    藉由lat,lon返回accuweather的天氣資訊
    '''
    def __init__(self,time,db_info,db_name,lat,lon,log_path):
        self.lat = lat
        self.lon = lon
        self.log_path = log_path
        self.language = 'en-us'
        self.details = 'true'
        self.metric = 'true'
        self.db_info = db_info
        self.db_name = db_name
        self.q = str(lat)+','+str(lon)


    # 修改api_key 狀態,一個api_key只可以使用30次,用完修改status狀態
    def mod_api_key(self,api_key):
        '''
        '''
        sql = "UPDATE Accu_key SET status=0 WHERE api_key='{}'".format(api_key)
        try:
            connect_DB(self.db_info,self.db_name,sql,'update',1)
            return True
        except:
            return False


    # 拉取位置代碼
    def get_location_code(self,api_key):
        '''
        '''
        while 1:
            local_url = "http://dataservice.accuweather.com/locations/v1/cities/geoposition/search?"
            location_url = local_url + "apikey=" + api_key + "&q="+ self.q + "&language=" + self.language + "&details=" + self.details + "&metric=" + self.metric
            response = requests.get(location_url)
            status,api_key = accuweather_api.check_response(self,response,api_key)
            if status:
                location = response.json()
                location_code = location['Details']['Key']
                return location_code,api_key
            else :
                continue

    
    def check_response(self,response,api_key):
        '''
        查看reponse status,並做出相對應動作
        '''
        if response.status_code == 200:
            return True,api_key
        else :
            accuweather_api.mod_api_key(self,api_key)
            api_key = get_api_key(self.db_info,self.db_name)
            if response.status_code == 503:
                pass
            else:
                go_to_log(self.log_path,'api_key invalid:{}'.format(response.status_code))
            return False,api_key


    #取得現在天氣data
    def get_now_data(self,api_key,location_code):
        '''
        '''
        while 1:
            url = "http://dataservice.accuweather.com/currentconditions/v1/"
            url += location_code + "?apikey=" + api_key + "&language=" + self.language + "&details=" + self.details + "&metric=" + self.metric
            response = requests.get(url)
            status,api_key = accuweather_api.check_response(self,response,api_key)
            if status:
                result = response.json()
                temp = result[0]["Temperature"]["Metric"]["Value"]
                ws = round(result[0]["Wind"]["Speed"]["Metric"]["Value"]* 5 / 18, 2)
                wd = result[0]["Wind"]["Direction"]["Degrees"]
                rh = result[0]["RelativeHumidity"]
                rf = result[0]["PrecipitationSummary"]["Precipitation"]["Metric"]["Value"]
                return temp,ws,wd,rh,rf,api_key
            else:
                continue


    #取得預測天氣data
    def get_pre_data(self,api_key,location_code):
        '''
        '''
        pre_time =[]
        pre_temp = []
        pre_ws = []
        pre_wd = []
        pre_rh =[]
        while 1 :
            url = "http://dataservice.accuweather.com/forecasts/v1/hourly/12hour/"
            url += location_code + "?apikey=" + api_key + "&language=" + self.language + "&details=" + self.details + "&metric=" + self.metric
            response = requests.get(url)
            status,api_key = accuweather_api.check_response(self,response,api_key)
            if status:
                results = response.json()
                for result in results :
                    pre_time.append(datetime.strptime(result["DateTime"],'%Y-%m-%dT%H:00:00+08:00'))
                    pre_temp.append(result["Temperature"]["Value"])
                    pre_ws.append((round(result["Wind"]["Speed"]["Value"]* 5 / 18, 2)))
                    pre_wd.append(float(result["Wind"]["Direction"]["Degrees"]))
                    pre_rh.append(result["RelativeHumidity"])
                return pre_time,pre_temp,pre_ws,pre_wd,pre_rh,api_key
            else :
                continue


def get_api_key(db_info,db_name):
        '''
        拉取api—key
        '''
        sql = "SELECT api_key FROM Accu_key WHERE status=1 ORDER BY id DESC"
        result = connect_DB(db_info,db_name,sql,'select',1)[0]
        return result


def return_key_status(db_info,db_name,hour):
    '''
    重置api_key status
    '''
    if datetime.now().hour == hour:
        sql = "UPDATE Accu_key SET status=1 WHERE status=0"
        connect_DB(db_info,db_name,sql,'update',0)


def go_to_log(log_path,e):
    '''
    寫進log
    '''
    with open(log_path,'a', newline='') as f:
        f.write('{} :{}\n'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),e))


def connect_DB(db_info, dbname, sql, sql_type, fetch):
    '''
    select 和 insert 資料庫操作\n
    db_info: secret\n
    db_name: 要操作的db名稱\n
    sql: sql語法\n
    sql_type: chose select or insert\n
    fetch:fetch all or fetch one
    '''
    conn = MySQLdb.connect(host=db_info[0],user=db_info[1],passwd=db_info[2],port=db_info[3],db=dbname)
    cur = conn.cursor()
    cur.execute(sql)
    if sql_type == 'select':
        if fetch == 0:
            result = cur.fetchall()
        else:
            result = cur.fetchone()
        cur.close()
        conn.commit()
        conn.close()
        return result
    elif sql_type in ('insert','delete','update'):
        cur.close()
        conn.commit()
        conn.close()


def catch_tower_data(segid,db_info,dbname,log_path):
    '''
    拉取計算dtr所需資料和電塔座標等參數
    '''
    # 拉取電塔座標與海拔
    sql = "SELECT SegID,Latitude,Longitude,Altitude,RouteID,TowerOrder FROM Segment WHERE SegID = '{}'".format(segid)
    result = connect_DB(db_info,dbname,sql,'select',0)
    # 拉取鄰近電塔座標
    neighbor_tower_sql = "SELECT Latitude,Longitude FROM Segment WHERE RouteID = {} AND TowerOrder > {}".format(result[0][4],result[0][5])
    neighbor_tower = connect_DB(db_info,dbname,neighbor_tower_sql,'select',1)
    # 如果是最後一座電塔,則拉取上一座電塔
    if neighbor_tower == None:
        neighbor_tower_sql = "SELECT Latitude,Longitude FROM Segment WHERE RouteID = {} AND TowerOrder < {}".format(result[0][4],result[0][5])
        neighbor_tower = connect_DB(db_info,dbname,neighbor_tower_sql,'select',1)
    # 拉取線徑
    sql = "SELECT diameter,R_high,R_low FROM STR WHERE CableType = (SELECT CableType FROM RouteInfo WHERE RouteID = {})".format(result[0][4])
    diameter = connect_DB(db_info,dbname,sql,'select',1)
    results = {"segid":result[0][0],"lat":result[0][1],"lon":result[0][2],"Alt":result[0][2],"Nlat":neighbor_tower[0],"Nlon":neighbor_tower[1],"cab":diameter[0],"R_high":diameter[1],"R_low":diameter[2]}
    return results


def check_time(db_info,dbname,tablename,time):
    '''
    檢查資料庫是否有最新資料
    '''
    sql = "SELECT time FROM {} ORDER BY time DESC LIMIT 1".format(tablename)
    result = connect_DB(db_info,dbname,sql,'select',1)[0].strftime("%Y-%m-%d %H:00:00")
    if result == time.strftime("%Y-%m-%d %H:00:00"):
        return False
    else :
        return True


def calculate_DTR(time,temp,ws,wd,rh,Tc,He,p1,p2,D0,R_high,R_low):
    '''
    計算DTR
    '''
    if -1 not in (temp,ws,wd,rh,Tc,He,p1,p2,D0,R_high,R_low) :
        scandate = time + timedelta(hours =-1)
        day = scandate.strftime("%m/%d/%Y")
        h = scandate.hour
        #計算風向與線段夾角
        phi = PHI(p1,p2,wd)
        DTR=round(Solve_I(Tc,temp,He,ws,day,h,p1,p2,D0,phi,R_high,R_low),2)
    else :
        DTR = -1
    return DTR


def acc_weather_api(time,segid,api_key,log_path,dbname):
    '''
    主控function
    '''
    tower_info = catch_tower_data(segid,ref.db_info,dbname,log_path)
    He = tower_info["Alt"] #海拔
    D0 = tower_info["cab"] #線徑
    p1 = [tower_info["lon"],tower_info["lat"]] #電塔座標
    p2 = [tower_info["Nlon"],tower_info["Nlat"]] #鄰近電塔座標
    R_high = tower_info["R_high"] # 線溫70度電阻
    R_low = tower_info["R_low"] # 線溫20度電阻
    sql = "INSERT INTO `{}`(time,WS,WD,temp,DTR,RH) VALUES ".format(segid)
    pre_sql = "INSERT INTO {}(cur_time,pre_time,WS,WD,temp,DTR,RH) VALUES ".format(segid)
    pre_DTR = []
    # 拉取D氣象所需參數 ws,wd,temp,rh
    acc = accuweather_api(time,ref.db_info,ref.select_db_name,tower_info["lat"],tower_info["lon"],log_path)
    location_code,api_key = acc.get_location_code(api_key)
    
    # 拉取現在氣象資料 temp,ws,wd,rh,rf
    temp,ws,wd,rh,rf,api_key = acc.get_now_data(api_key,location_code)

    if dbname == 'Gridwell':
        # 拉取預測氣象資料 time,temp,ws,wd,rh,rf
        pre_time,pre_temp,pre_ws,pre_wd,pre_rh,api_key = acc.get_pre_data(api_key,location_code)

    # 計算現在DTR
    now_DTR = calculate_DTR(time,temp,ws,wd,rh,ref.Tc,He,p1,p2,D0,R_high,R_low)
    
    # 計算預測DTR & 組合predict DTR insert sql
    if dbname == 'Gridwell':
        for i in range(len(pre_time)):
            DTR = calculate_DTR(pre_time[i],pre_temp[i],pre_ws[i],pre_wd[i],pre_rh[i],ref.Tc,He,p1,p2,D0,R_high,R_low)
            pre_DTR.append(DTR)
            pre_sql2 = "('{}','{}',{},{},{},{},{}),".format(time.strftime("%Y-%m-%d %H:00:00"),pre_time[i].strftime("%Y-%m-%d %H:00:00"),pre_ws[i],pre_wd[i],pre_temp[i],pre_DTR[i],pre_rh[i])
            pre_sql += pre_sql2
        pre_sql = pre_sql[:-1]
        #insert 預測data 2 DB
        connect_DB(ref.db_info,ref.pre_db_name,pre_sql,'insert',0)
        # connect_DB(ref.db_info2,ref.pre_db_name,pre_sql,'insert',0)
        sql = "INSERT INTO `{}`(time,WS,WD,temp,DTR,RH) VALUES ".format(segid) 
        now_sql = sql +"('{}',{},{},{},{},{})".format(time.strftime("%Y-%m-%d %H:00:00"),ws,wd,temp,now_DTR,rh)

    elif dbname == 'TowerBase_Gridwell':
        sql = "INSERT INTO `{}`(time,WS,WD,temp,DTR,RH,rainfall) VALUES ".format(segid)
        now_sql = sql +"('{}',{},{},{},{},{},{})".format(time.strftime("%Y-%m-%d %H:00:00"),ws,wd,temp,now_DTR,rh,rf)

    #insert現在data 2 DB    
    connect_DB(ref.db_info,ref.insert_db_name,now_sql,'insert',0)
    # connect_DB(ref.db_info2,ref.insert_db_name,now_sql,'insert',0)

    return api_key

