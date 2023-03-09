import datetime
import time

import pymysql
import os.path
import datetime as dt

import secret
import secret as s

# write a text file for Conky to cat


# Config
print_all_values = True
loop_code = False


class Get_Data:
    """Get values from my database.
    Construct a nice print.
     Parse into two files.
    Conky app will run concatenate (cat) the files"""

    def __init__(self, request: str) -> None:
        self.old_data = self.old_data()
        self.data = {}
        self.db_name = None
        self.table = None
        self.column = None
        self.msg = None

        # TODO, while True loop?

        self.data = self.collect_data()
        if print_all_values:
            self.print_data()
        self.parse_data()

    def parse_data(self):
        e = self.parse_economy()
        w = self.parse_weather()

        if e and w:
            # print("old data?", self.old_data, self.old_data['ts'])
            self.msg = self.data['ts']
            self.writefile("created")
        else:
            print("Error writing files")

    def parse_economy(self):
        try:
            # TODO add weekday and week number
            l1 = "1 EUR = " + str(self.data['sek']) + " SEK / " + str(self.data['usd']) + " USD"
            l2 = "1 BTC = " + str(self.data['btc'] + " USD")
            l3 = "TS in DB: " + str(self.old_data['ts']) + " (" + str(self.old_data['age']) + " min old)"
            self.msg = l1 + "\n" + l2 + "\n" + l3
            self.writefile("economy")
            return True

        except Exception as e:
            self.msg = "Error:" + str(e)
            self.writefile("economy")
            return False

    def parse_weather(self):
        # TODO
        print("Hej")
        return True

    def writefile(self, filename):
        # also make created file (so I can determine if its old data)
        with open(secret.file_path() + str(filename) + ".txt", "w") as f:
            f.write(str(self.msg))

    def collect_data(self):
        d = {}
        self.db_name = s.db_name1()
        self.column = s.column1()
        tn = "weather_"
        self.table = tn + s.table1()
        d['datarum'] = self.fetcher()[0][0]
        self.table = tn + s.table2()
        d['sovrum'] = self.fetcher()[0][0]
        self.table = tn + s.table3()
        d['kitchen'] = self.fetcher()[0][0]

        # return msg
        self.table = tn + s.table4()
        d['outside'] = self.fetcher()
        self.column = "*"
        all_w = self.fetcher()[0]

        d['tot_entires'] = all_w[0]
        d['ts'] = all_w[1]
        d['outside'] = all_w[3]
        d['humidity'] = all_w[4]
        d['snow_1'] = all_w[5]
        d['snow_3h'] = all_w[6]
        d['rain_1h'] = all_w[7]
        d['rain_3h'] = all_w[8]
        d['wind_speed'] = all_w[9]
        d['wind_deg'] = all_w[10]
        d['gust'] = all_w[11]
        d['cloud'] = all_w[12]
        d['sunrise'] = all_w[13]
        d['sunset'] = all_w[14]
        d['status'] = all_w[15]

        self.currency_enabled()
        if self.currency_enabled:
            self.table = s.table5()
            # euro to sek/usd
            all_cur = self.fetcher()
            d['usd'] = all_cur[0][1]
            d['sek'] = all_cur[1][1]

        self.db_name = s.db_name2()
        self.table = s.table6()
        d['btc'] = self.fetcher()[0][0]

        return d

    def fetcher(self):
        h, u, p = s.sql()
        db = pymysql.connect(host=h, user=u, passwd=p, db=self.db_name)
        c = db.cursor()
        sql_data = None

        try:
            c.execute(self.sql_query())
            sql_data = c.fetchall()
            # print("Raw data:", sql_data)
            c.close()

        except pymysql.Error as e:
            print("Error reading DB: %d: %s" % (e.args[0], e.args[1]))
            print("nothingness")
            print("!!", self.db_name)

        if sql_data:
            return sql_data

        else:
            print("found no values")
            pass

    def sql_query(self):
        if self.table == s.table5():
            return "SELECT Currency, Rate, time FROM currency_rate WHERE Currency='SEK' " \
                   "OR Currency='USD' ORDER BY time DESC LIMIT 2"

        if self.table == s.table6():
            return "SELECT {} FROM {} ORDER BY Time DESC LIMIT 1".format(self.column, self.table)

        else:
            return "SELECT {} FROM {} ORDER BY value_id DESC LIMIT 1".format(self.column, self.table)

    def currency_enabled(self):
        # only get values between certain time (db updates 08:05 and 17:05)
        # except if no data or old data in files
        timenow = dt.datetime.now().time()
        if dt.time(7, 45) < timenow < dt.time(8, 30):
            return True
        elif dt.time(16, 45) < timenow < dt.time(17, 30):
            return True
        elif self.old_data['old']:
            return True
        else:
            return False

    def old_data(self):
        og = {'old': bool, 'ts': datetime.datetime, 'age': int}
        log_file = secret.file_path() + "created.txt"
        if os.path.isfile(log_file):
            with open(log_file, "r") as file:
                ts = dt.datetime.strptime(file.read(), '%Y-%m-%d %H:%M:%S')
                og['ts'] = ts
                dur = dt.datetime.now() - ts
                duration = round((dur.total_seconds() / 60))
                og['age'] = duration
                # check if data older than 15 min and return True/False statement
                if duration > 20:
                    og['old'] = True
                else:
                    og['old'] = False
        else:
            og['old'] = True
            og['ts'] = None
            print("No files previous files created at:", log_file)
        return og

    def print_data(self):
        print("data:\nKey : Value : Datatype")
        for x in self.data:
            print(x, ":", self.data[x], ":", type(self.data[x]))


def start():
    print("Feeder program starts")

    # TODO this loop is broken
    # just check when last run was made
    # if old, fetch new data
    # now it loops whole code every 5th seconds
    while loop_code:
        x = Get_Data("weather")
        timer = x.old_data
        calc = 15 - x.old_data['age']
        print("age:", x.old_data['age'], "execute in", calc)
        time.sleep(5)
        #if timer['age'] > 15:
        if timer['age'] > 8:
            print(timer['age'])
            Get_Data("weather")
        else:
            time.sleep(5)
    else:
        Get_Data("weather")
        pass

    # TODO
    #
    # add energy prices (?)
    # Parse and render two files (weather.txt, economy.txt)
    # run code as a systemd service (systemctl), add wait times



if __name__ == "__main__":
    start()
