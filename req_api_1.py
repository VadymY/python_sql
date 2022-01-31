
import requests

import datetime
import os
import time
from datetime import timedelta
import sqlite3
from sqlite3 import Error

url = 'https://api.openweathermap.org/data/2.5/onecall/timemachine'
headers = {'Accept': 'application/json'}
# # 76be3b5c44f69d0be7a27f80a70cc0a3
# 79f071aeeaa26accbdc59e42a12719ca

# api.openweathermap.org/data/2.5/forecast/daily?q={city name}&cnt={cnt}&appid={API key}
class City:
    def __init__(self, town, lat, lon):
        self.city = town
        self.lon = lon
        self.lat = lat
        self.json_set = []

    def set_json(self, json):
        self.json_set.append(json)

def create_connection(path):
    connection = None
    try:
        connection = sqlite3.connect(path)
        print("Connection to SQLite DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.executescript(query)
        cursor.close()
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")

def execute_query_ret_simple(connection, query):
    cursor = connection.cursor()
    ret = None
    try:
        res = cursor.execute(query)
        ret = res.fetchall()
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")
    return ret

def avg(ar):
    sum = 0
    for i in ar:
        sum += i
    return sum / len(ar)

db_filename = "local_day_temp.sqlite"
path = os.path.join(os.path.abspath(os.path.dirname(__file__)), db_filename)
try:
    os.remove(path)
except:
    print("DB ", db_filename, " not found")
print("-----------------------------------")
print(path)
print("-----------------------------------")
connection = create_connection("local_day_temp.sqlite")

create_cities_table = """
CREATE TABLE IF NOT EXISTS cities (
  id INT AUTO_INCREMENT PRIMARY KEY NOT NULL, city VARCHAR(30), lat float, lon float
);
"""

gg = (time.time())
cities = []
cities.append(City("Kyiv", 50.447731, 30.542721))
cities.append(City("Odessa", 46.482525, 30.723309))
cities.append(City("Lviv", 49.839684, 24.029716))
cities.append(City("London", 51.507351, -0.127758))
cities.append(City("Krakow", 50.0797198, 19.960990))
cities.append(City("Manchester", 53.479801, -2.244214))
cities.append(City("Wrocław", 51.1186688, 17.030978))
cities.append(City("Rīga", 56.94924888, 24.1167258))
cities.append(City("Tallinn", 59.4376067, 24.737964))
cities.append(City("Kaunas", 54.9095765, 23.91290376))

dt_req = []
dt_days = []
for i in range(-5, 0):
    gop = (datetime.datetime.utcnow() + timedelta(days=i))
    dt_req.append(gop.timestamp())
    dt_days.append(str(gop.date()))

base_req = 'http://api.openweathermap.org/data/2.5/onecall/timemachine'
# api_key = '79f071aeeaa26accbdc59e42a12719ca'
api_key = '4444feba7d460744f903e2290b02a587'
for i in cities:
    for dt in dt_req:
        req_str = "{:s}{:s}{:.2f}{:s}{:.2f}{:s}{:d}{:s}{:s}".format(base_req, "?lat=", i.lat, '&lon=', i.lon,'&dt=', int(dt), '&units=metric&appid=', api_key)
        req_7 = requests.get(req_str)
        if req_7.status_code == 200:
            i.set_json(req_7.json())
        else:
            raise RuntimeError(req_7.reason)

tm = cities[0].json_set[0]['hourly'][0]['temp']

execute_query(connection, create_cities_table)
sep = ', '
cnt = 1
for i in cities:
    insert_city_req = """
    INSERT INTO cities(id, city, lat, lon) values(
    """
    insert_city_req_test = """
        INSERT INTO cities(id, city, lat, lon) values(1, "Odessa", 49.67, 30.87 );
        """
    city = '"' + i.city + '"';
    insert_city_req += '{:d}{:s}{:s}{:s}{:.2f}{:s}{:.2f});'.format(cnt, sep, city, sep, i.lat, sep, i.lon)
    # execute_query(connection, insert_city_req_test)
    execute_query(connection, insert_city_req)
    cnt += 1

insert_days_req = 'create table IF NOT EXISTS five_days(id INT AUTO_INCREMENT PRIMARY KEY NOT NULL, day_1 INT, day_2 INT, day_3 INT, day_4 INT, day_5 INT);'
execute_query(connection, insert_days_req)

insert_hours_1_req = "create table IF NOT EXISTS first_day"
insert_hours_2_req = "create table IF NOT EXISTS second_day"
insert_hours_3_req = "create table IF NOT EXISTS third_day"
insert_hours_4_req = "create table IF NOT EXISTS fourth_day"
insert_hours_5_req = "create table IF NOT EXISTS fifth_day"
day_hours = """(id INT AUTO_INCREMENT PRIMARY KEY NOT NULL, 
hour_1 int, hour_2 int, hour_3 int, hour_4 int, hour_5 int, hour_6 int, 
hour_7 int, hour_8 int, hour_9 int, hour_10 int, hour_11 int, hour_12 int, 
hour_13 int, hour_14 int, hour_15 int, hour_16 int, hour_17 int, hour_18 int, 
hour_19 int, hour_20 int, hour_21 int, hour_22 int, hour_23 int, hour_24 int, dat date);
"""

execute_query(connection, insert_hours_1_req + day_hours)
execute_query(connection, insert_hours_2_req + day_hours)
execute_query(connection, insert_hours_3_req + day_hours)
execute_query(connection, insert_hours_4_req + day_hours)
execute_query(connection, insert_hours_5_req + day_hours)

create_dedup_req = "create table IF NOT EXISTS dedup(id INT AUTO_INCREMENT PRIMARY KEY NOT NULL, temp float);"
execute_query(connection, create_dedup_req)
# execute_query(connection, "INSERT INTO dedup(id, temp) VALUES (1, -3.5), (2, -12.3), (3, 3.8);")

cnt = 1
str_days_req = 'INSERT INTO five_days(id, day_1, day_2, day_3, day_4, day_5) values ('
for i in range(1, len(cities) + 1):
    str = str_days_req + '{:d}{:s}{:d}{:s}{:d}{:s}{:d}{:s}{:d}{:s}{:d});'.format(cnt, sep, i, sep, i, sep, i, sep, i, sep, i)
    execute_query(connection, str)
    cnt += 1

set_temp = set()
for i in cities:
    for j in i.json_set:
        for t in j['hourly']:
            set_temp.add(t['temp'])
lst_temp = sorted((list(set_temp)))

str_dedup = "INSERT INTO dedup(id, temp) VALUES "
cnt = 1
for i in lst_temp:
    if cnt < len(lst_temp):
        str_dedup += '({:d}{:s}{:.2f}){:s}'.format(cnt, sep, i, sep)
    else:
        str_dedup += '({:d}{:s}{:.2f});'.format(cnt, sep, i)
    cnt += 1

print(str_dedup)
execute_query(connection, str_dedup)

str_req_day = ["INSERT INTO first_day", "INSERT INTO second_day", "INSERT INTO third_day", "INSERT INTO fourth_day", "INSERT INTO fifth_day"]
str_hours = """(id, 
hour_1 , hour_2 , hour_3 , hour_4 , hour_5 , hour_6 , 
hour_7 , hour_8 , hour_9 , hour_10 , hour_11 , hour_12 , 
hour_13 , hour_14 , hour_15 , hour_16 , hour_17 , hour_18 , 
hour_19 , hour_20 , hour_21 , hour_22 , hour_23 , hour_24, dat) 
values ("""
cnt = 1
for i in cities:
    day = 0
    for j in i.json_set: # 5 days
        str = str_req_day[day] + str_hours + "{:d}{:s}".format(cnt, sep)
        hour = 1
        dat = dt_days[day]
        for t in j['hourly']:
            idx = lst_temp.index(t['temp']) + 1
            if hour < len(j['hourly']):
                str += "{:d}{:s}".format(idx, sep)
            else:
                str += '{:d}{:s}"{:s}");'.format(idx, sep, dat)
            hour += 1
        execute_query(connection, str)
        day += 1
    cnt += 1

# connection.close()
# # /////////////////////////////
# connection = create_connection("local_day_temp.sqlite")


loc_days_temp = []
day_names = ["first_day", 'second_day', 'third_day', 'fourth_day', 'fifth_day']
day_nums = ["day_1", 'day_2', 'day_3', 'day_4', 'day_5']

# execute_query(connection, "use local_day_temp;")
lat_lon = []
ret = execute_query_ret_simple(connection, "select lat, lon from cities")
l = len(ret)
for i in ret:
    lat_lon.append(i)


date_db_all = []
for i in range(5):
    # str_dat = "use local_day_temp; "
    str_dat = 'select {0}'.format(day_names[i]) + '.dat from {0}'.format(day_names[i]) + ' where {0}'.format(day_names[i]) + '.id = {0};'.format(i + 1)
    ret = execute_query_ret_simple(connection, str_dat)
    date_db_all.append(ret[0][0])

str_test = "SELECT dedup.temp FROM cities, five_days, dedup,"
str_next_1 = " WHERE cities.id = five_days.id and cities.id = "
str_next_2 = ' and five_days.'
for i in range(1, 11):
    all_days = []
    for j in range(0, 5):
        days = []
        for k in range(1, 25):
            str_req = str_test
            str_req += day_names[j] + str_next_1 +'{:d}'.format(i) + str_next_2 + '{:s}'.format(day_nums[j]) + ' = {:s}.id and '.format(day_names[j])
            str_req += '{0}.hour_{1}  = dedup.id;'.format(day_names[j], k)
            ret = execute_query_ret_simple(connection, str_req)
            retf = float(ret[0][0])
            days.append(retf)
        mx = max(days)
        mn = min(days)
        aver = avg(days)
        all_days.append((mx, mn, aver))
    loc_days_temp.append(all_days)
h = 4

res_dedup = execute_query_ret_simple(connection, "select temp from dedup;")
dedup_list = []
for i in res_dedup:
    dedup_list.append(i)

avg_days = []
mn_pos_temp = []
mx_pos_temp = []
mx_real_list = []

mn_days_array = []
mx_days_array = []
for cnt in range(5):
    sum_avg = 0
    mx_list = []
    mn_list = []
    for i in loc_days_temp:
        mx, mn, av = i[cnt]
        sum_avg += av
        mn_list.append(mn)
        mx_list.append(mx)
    sum_avg /= len(i[cnt])
    mn_real = min(mn_list)
    mx_real = max(mx_list)
    mx_real_list.append(mx_real)
    mn_pos = mn_list.index(mn_real)
    mx_pos = mx_list.index(mx_real)
    mn_days_array.append(mn_list)
    mx_days_array.append(mx_list)

    avg_days.append(sum_avg)
    mn_pos_temp.append(mn_pos)
    mx_pos_temp.append(mx_pos)

db_filename_2 = "calc_db.sqlite"
try:
    path = os.path.join(os.path.abspath(os.path.dirname(__file__)), db_filename_2)
    os.remove(path)
except:
    print("filename", db_filename_2, "is not exist")

connection2 = create_connection(db_filename_2)

str_cr_tbl_1 = """create table data_set_1(
id INT AUTO_INCREMENT NOT NULL PRIMARY KEY, lat float, lon float, dat date, mx_tmp_loc float,  mx_tmp_month float);"""

execute_query(connection2, str_cr_tbl_1)

str_cr_tbl_2 = """create table data_set_2(
id INT AUTO_INCREMENT NOT NULL PRIMARY KEY, avg_d float, min_tmp float, mn_tmp_lat float, mn_tmp_lon float, max_tmp float, mx_tmp_lat float, mx_tmp_lon float);"""

execute_query(connection2, str_cr_tbl_2)

str_insert_tbl_2 = 'insert into data_set_2(id, avg_d, min_tmp, mn_tmp_lat, mn_tmp_lon, max_tmp, mx_tmp_lat, mx_tmp_lon)'

for i in range(5):
    mn_pos = mn_pos_temp[i]
    mx_pos = mx_pos_temp[i]
    str_req2 = str_insert_tbl_2
    mx_temp = mx_days_array[i][mx_pos]
    mn_temp = mn_days_array[i][mn_pos]
    str_req2 += " values ({:d}, {:f}, {:f}, {:f}, {:f}, {:f}, {:f}, {:f});".format(i + 1, avg_days[i], mn_temp, lat_lon[mn_pos][0], lat_lon[mn_pos][1],
                                                                              mx_temp, lat_lon[mx_pos][0], lat_lon[mx_pos][1])
    execute_query(connection2, str_req2)

mx_month = max(mx_real_list)
str_insert_tbl_1 = "insert into data_set_1(id, lat, lon, dat, mx_tmp_loc, mx_tmp_month) values("

for i in range(5):
    str_next = str_insert_tbl_1
    mx_pos = mx_pos_temp[i]
    lat, lon = lat_lon[mx_pos]
    dt = date_db_all[i]
    mx_temp = mx_days_array[i][mx_pos]
    str_next += '{:d}, {:f}, {:f}, '.format(i + 1, lat, lon)
    str_next += '"{:s}"'.format(dt)
    str_next += ', {:f}, {:f});'.format(mx_temp, mx_month)
    execute_query(connection2, str_next)

connection.close()
connection2.close()