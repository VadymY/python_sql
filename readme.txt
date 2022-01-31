I fulfil this test.

My app based on python language and used SQLite database.

At first, I read data from weather site using API key. All data I saving in the database local_day_temp.sqlite;
I am using  deduplication therefore I save unique set of temperature in the table dedup.
So, for reference from tables first_day, second_day, third_day, fourth_day and fifth day I am usinf indecies.

The second part of my task contain action for receiving data from this sql table (local_day_temp.sqlite) and calculation concerning to described algoritmes and savind data into  calc_db.sqlite data base.

The first table  data_set_1 contains:  location of maximum temperature per day from all locations (10) and maximum temperature per 5 days (I mean the month).
The second table data_set_2 contains: avg_d - average temperature of all locations (10) for each from 5 days,
                                      min_tmp - minimum temperature of all locations (10) for each from 5 days,
									  mn_tmp_lat - latitude of location with minimum temperature for each from 5 days,
									  mn_tmp_lon - longitude of location with minimum temperature for each from 5 days,
									  max_tmp - maximum temperature of all locations (10) for each from 5 days,
									  mx_tmp_lat - latitude of location with maximum temperature for each from 5 days,
									  mx_tmp_lon - longitude of location with maximum temperature for each from 5 days,

I applyied 5 files: req_api_1.py, Dockerfile, local_day_temp.sqlite and calc_db.sqlite and readme.txt

Checking and building (Windows):
1. Copy req_api_1.py, Dockerfile to work directory (wd).
2. "docker build ."
3. "docker images"
    and select ID of last image (IDLI)
4. "docker exec -ti <IDLI> bash"
5. "pwd"
   You will see similar to:
    root@123456:/= /app
6. "python req_api_1.py"  and wait for finish	

7. Start other terminal: "cmd"
8. docker cp "123456:/= /app/local_day_temp.sqlite" .
9. docker cp "123456:/= /app/calc_db.sqlite" .
10. You can find in the current folder the both files of data base.
