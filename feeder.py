import datetime
import sys
import time
import logging

import pymysql
import os.path
import datetime as dt
from datetime import timedelta

import secret as s
# write a text file for Conky to cat

# Config
print_all_values, loop_code = s.settings()
logging.basicConfig(level=logging.INFO, filename="log.log", filemode="w",
                    format="%(asctime)s - %(levelname)s - %(message)s")
create_file_log = True


class Get_Data:
    """Get values from my database.
    Construct a nice print.
     Parse into two files.
    Conky app will run concatenate (cat) the files"""

    def __init__(self, old_data: dict) -> None:
        self.old_data = old_data
        self.data = {}
        self.db_name = None
        self.table = None
        self.column = None
        self.msg = None

        self.data = self.collect_data()
        if print_all_values:
            self.print_data()
        self.parse_data()

    def parse_data(self):
        e = self.parse_economy()
        w = self.parse_weather()

        if e and w:
            self.msg = self.data['ts']
            self.writefile("created")
        else:
            logging.error("Error writing files")
            print("Error writing files")

    def parse_economy(self):
        try:
            ts = dt.datetime.now()
            l1 = "1 EUR = " + str(self.data['sek']) + " SEK / " + str(self.data['usd']) + " USD"

            # btc sum; remove decimal
            btc_sum = int(self.data['btc'][2])
            # add spacing for thousands
            btc_sum = f"{btc_sum:,}"
            btc_sum = btc_sum.replace(',', ' ')

            l2 = "1 BTC = " + str(btc_sum) + " USD (" + str(self.data['btc'][16]) + " %)"

            try:
                l3 = self.parse_nordpool()
            except Exception as e:
                logging.exception("could not get NordPool stats:")
                logging.exception(str(e))
                l3 = "Could not get NordPool stats"

            tot = str(self.data['tot_entries'])
            tot = tot[0:2] + ' ' + tot[2:]
            l4 = "TS from DB: " + str(self.data['ts']) + ", total: " + str(tot)

            days = ["Monday", "Tuesday", "Wednesday",
                    "Thursday", "Friday", "Saturday", "Sunday"]
            l5 = str(days[ts.weekday()]) + ", week " + str(ts.isocalendar().week)

            self.msg = l1 + "\n" + l2 + "\n" + l3 + "\n" + l4 + "\n" + l5
            self.writefile("economy")
            if print_all_values:
                print("Created economy.txt")
            return True

        except Exception as e:
            self.msg = "Error:" + str(e)
            print("ERROR:", e)
            self.writefile("economy")
            return False

    def parse_weather(self):
        def formatter(line):
            c2 = len(line[1])
            c1 = 54 - c2
            format_string = "{: <" + str(c1) + "}{: >" + str(c2) + "}\n"
            return format_string

        d = self.data

        # column 1, line 1
        if d['cloud']:
            r1c1 = str(d['status']) + ", clouds: " + str(d['cloud']) + "%"
        else:
            r1c1 = str(d['status'])

        # column 1, line 2
        if d['gust']:
            x = ", gust: " + d['gust']
        else:
            x = ""
        r2c1 = "wind: speed: " + str(d['wind_speed']) + ", deg: " + str(d['wind_deg']) + str(x)

        # column 1, line 3
        r3c1 = None
        if d['rain_1h']:
            if d['rain_3h']:
                r3c1 = "Rain 1h: " + str(d['rain_1h']) + " Rain 3h: " + str(d['rain_3h'])
            else:
                r3c1 = "Rain 1h: " + str(d['rain_1h'])
        elif d['snow_1h']:
            if d['snow_3h']:
                r3c1 = "Snow 1h: " + str(d['snow_1h']) + " Snow 3h: " + str(d['snow_3h'])
            else:
                r3c1 = "Snow 1h: " + str(d['snow_1h'])
        elif not r3c1:
            r3c1 = ""

        # column 1, line 5
        r4c1 = "Humidity, out: " + str(d['humidity']) + "%, datarum: " + str(round(d['datarum_h'])) + "%"

        # column 1, line 4
        r5c1 = "Daylight: " + str(d['sunrise'] + dt.timedelta(hours=1))[:-3] + " -> " \
               + str(d['sunset'] + dt.timedelta(hours=1))[:-3]

        txt = [
            [r1c1, "Kitchen: " + str(d['kitchen']) + "°C"],
            [r2c1, "Datarum: " + str(d['datarum']) + "°C"],
            [r3c1, "Lowes: " + str(d['sovrum']) + "°C"],
            [r4c1, "Outside: " + str(d['outside']) + "°C"],
            [r5c1, ""]
        ]

        self.msg = ""
        for row in txt:
            format_line = formatter(row)
            self.msg = self.msg + format_line.format(*row)

        self.writefile("weather")
        if print_all_values:
            print("Created weather.txt")
        return True

    def writefile(self, filename):
        # also make created.txt file (so I can determine if its old data)
        with open(s.file_path() + str(filename) + ".txt", "w") as f:
            f.write(str(self.msg))

    def collect_data(self):
        d = {}
        self.db_name = s.db_name1()
        self.column = s.column1()
        tn = "weather_"
        self.table = tn + s.table1()
        data = self.fetcher()[0]
        d['datarum'] = data[0]
        d['datarum_h'] = data[1]
        self.table = tn + s.table2()
        d['sovrum'] = self.fetcher()[0][0]
        self.table = tn + s.table3()
        d['kitchen'] = self.fetcher()[0][0]

        # return msg
        self.table = tn + s.table4()
        d['outside'] = self.fetcher()
        self.column = "*"
        all_w = self.fetcher()[0]

        d['tot_entries'] = all_w[0]
        d['ts'] = all_w[1]
        d['outside'] = all_w[3]
        d['humidity'] = all_w[4]
        d['rain_1h'] = all_w[5]
        d['rain_3h'] = all_w[6]
        d['snow_1h'] = all_w[7]
        d['snow_3h'] = all_w[8]
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
            if all_cur[0][1] > all_cur[1][1]:
                d['usd'] = all_cur[1][1]
                d['sek'] = all_cur[0][1]
            else:
                d['usd'] = all_cur[0][1]
                d['sek'] = all_cur[1][1]

        self.db_name = s.db_name2()
        self.table = s.table6()
        #btc = self.fetcher()[0]
        d['btc'] = self.fetcher()[0]
        #d['btc'] = btc
        # get NordPool (price atm, day average)
        self.table = "NordPool"
        d['NordPool'] = self.fetcher()

        return d

    def fetcher(self):
        h, u, p = s.sql()
        db = pymysql.connect(host=h, user=u, passwd=p, db=self.db_name)
        c = db.cursor()
        sql_data = None

        try:
            c.execute(self.sql_query())
            sql_data = c.fetchall()
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
            return "SELECT {} FROM {} ORDER BY id DESC LIMIT 1".format(self.column, self.table)

        if self.table == "NordPool":
            return "SELECT * FROM NordPool ORDER BY value_id DESC LIMIT 48"

        else:
            return "SELECT {} FROM {} ORDER BY value_id DESC LIMIT 1".format(self.column, self.table)

    def currency_enabled(self):
        # only get values between certain time (db updates 08:05 and 17:05)
        # except if no data or old (5h) data in files
        time_now = dt.datetime.now().time()
        if dt.time(7, 45) < time_now < dt.time(8, 30):
            return True
        elif dt.time(16, 45) < time_now < dt.time(17, 30):
            return True
        elif 'age_min' in self.old_data:
            # check age threshold
            if self.old_data['age_min'] > 5 * 60:
                return True
            else:
                return False
        else:
            return False

    def parse_nordpool(self):
        # calculate mwh -> kwh. Display öre/kwh?
        # prices listed as kronor / megawatt hour(mwh)
        # 1 mwh to kwh = 1000 kwh

        # Find correct row for day average stats

        d = self.data['NordPool']

        # find recent value
        ts = datetime.datetime.now() - timedelta(hours=1)
        val = None
        for x in d:
            if x[2] < ts < x[3]:
                val = x[4]

        if val:
            # kr/MWh -> öre/kWh
            # 1 MWh = 1000 kWh, 1 kr = 1 000 öre
            price = round(val / 10, 2)
            txt = f"NordPool: {price} öre/kWh"
        else:
            txt = "Has no NordPool stats to show.."

        return txt

    def print_data(self):
        print("data:\nKey : Value : Datatype")
        for x in self.data:
            print(x, ":", self.data[x], ":", type(self.data[x]))


def start():
    print("Feeder program starts")
    try:
        while loop_code:
            old_data = pre_data()
            sleep = (15 * 60) - (old_data['age'] - 4)
            # no negative integers for sleep command pls
            if sleep <= 3:
                sleep = 3
            # pre_data changes the 'old' statement after 15 min
            # print user feedback to console
            if old_data['old']:
                print("Data is old (" + str(old_data['age_min']) + " min), Get new data")
                try:
                    Get_Data(old_data)
                except Exception as e:
                    print("could not launch, error:", e)
                    logging.exception(e)
                    continue
            else:
                if sleep < 60:
                    print("Data age:", old_data['age_min'], "min. Next run in:", round(sleep), "sec")
                else:
                    print("Data age:", old_data['age_min'], "min. Next run in:", round(sleep / 60), "min")

            time.sleep(sleep)
        else:
            old_data = pre_data()
            Get_Data(old_data)
            pass
    except Exception as e:
        logging.exception(e)
        pass

    # TODO
    #
    # add energy prices (?)
    # add car stats (?)


def pre_data():
    og = {'old': bool, 'ts': datetime.datetime, 'age': int}
    log_file = s.file_path() + "created.txt"
    if os.path.isfile(log_file):
        with open(log_file, "r") as file:
            ts = dt.datetime.strptime(file.read(), '%Y-%m-%d %H:%M:%S')
            og['ts'] = ts
            dur = dt.datetime.now() - ts
            og['age'] = dur.total_seconds()
            og['age_min'] = round(og['age'] / 60)
            # check if data is older than 15 min and 3 sec, return True/False statement
            if og['age'] > (15 * 60 + 3):
                og['old'] = True
            else:
                og['old'] = False
    else:
        og['old'] = True
        og['ts'] = None
        print("No files previous files created at:", log_file)
    return og


if __name__ == "__main__":
    logging.info("Program started")
    start()
