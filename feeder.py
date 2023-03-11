import datetime
import time

import pymysql
import os.path
import datetime as dt

import secret
import secret as s

# write a text file for Conky to cat


# Config
print_all_values = False
loop_code = False


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
            # print("old data?", self.old_data, self.old_data['ts'])
            self.msg = self.data['ts']
            self.writefile("created")
        else:
            print("Error writing files")

    def parse_economy(self):
        try:
            # TODO add weekday and week number, remove age min old
            l1 = "1 EUR = " + str(self.data['sek']) + " SEK / " + str(self.data['usd']) + " USD"
            l2 = "1 BTC = " + str(self.data['btc'] + " USD")
            l3 = "TS in DB: " + str(self.data['ts'])
            self.msg = l1 + "\n" + l2 + "\n" + l3
            self.writefile("economy")
            print("Created economy.txt")
            return True

        except Exception as e:
            self.msg = "Error:" + str(e)
            self.writefile("economy")
            return False

    def parse_weather(self):
        # TODO
        #left_alignment = "Left Text"
        #right_alignment = str(self.data['kitchen']) + " °C Kitchen"
        #l1 = f"{left_alignment : <20}{right_alignment : >20}"
        #print("Aligned text?\n", l1)

        def formatter(row):
            tot = 54

            c2 = len(row[1])
            # c1 = 54 - (c2 + len(row[0]))
            #l = len(row[0])
            #calc = 54 - (c2 + l)

            # TODO
            # c1 = 54 - c2
            # just leave the code running holder
            c1 = 52 - c2

            print("Row width:", c1, "+", c2, " sum:", c1 + c2)

            format_line = "{: <" + str(c1) + "}{: >" + str(c2) + "}\n"
            return format_line

        d = self.data

        x = str(d['status']) + ", clouds: " + str(d['cloud']) + "%"
        # r1c1 = ((" " * 20) - len(x))
        r1c1 = str(d['status']) + ", clouds: " + str(d['cloud']) + " %"
        r2c1 = "wind speed: " + str(d['wind_speed']) + " deg: " + str(d['wind_deg'])
        r3c1 = "Empty"
        r4c1 = "This is a very long sentence."

        txt = [
            [r1c1, "Kitchen: " + str(d['kitchen']) + "°C"],
            [r2c1, "Datarum: " + str(d['datarum']) + "°C"],
            [r3c1, "  Lowes: " + str(d['sovrum']) + "°C"],
            [r4c1, "Outside: " + str(d['outside']) + "°C"]
        ]

        c1 = str(38)
        c2 = str(16)

        format_line = "{: <" + str(c1) + "}{: >" + str(c2) + "}"

        # 37 + 15 ?
        self.msg = ""
        for row in txt:
            #    |37     | 8 | 6|
            format_line = formatter(row)
            #print(format_line)
            self.msg = self.msg + format_line.format(*row)

        self.writefile("weather")
        print("Created weather.txt")

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
        # or if it's older than 5 hours
        elif self.old_data['age_min'] > 5 * 60:
            return True
        else:
            return False

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

    old_data = pre_data()
    while loop_code:
        # pre_data changes the old statement after 15 min
        if old_data['old']:
            print("Get new data")
            Get_Data(old_data)
        else:
            pass
        old_data = pre_data()
        sleep = (15 * 60) - (old_data['age'] - 4)
        # no negative integers for sleep command pls
        if sleep <= 0:
            sleep = 0
        print("Data age:", old_data['age_min'], "min. Next run in:", round(sleep / 60), "min")
        time.sleep(sleep)
    else:
        Get_Data(old_data)
        pass

    # TODO
    #
    # add energy prices (?)
    # Parse and render two files (weather.txt, economy.txt)


def pre_data():
    og = {'old': bool, 'ts': datetime.datetime, 'age': int}
    log_file = secret.file_path() + "created.txt"
    if os.path.isfile(log_file):
        with open(log_file, "r") as file:
            ts = dt.datetime.strptime(file.read(), '%Y-%m-%d %H:%M:%S')
            og['ts'] = ts
            dur = dt.datetime.now() - ts
            age_sek = dur.total_seconds()
            age_min = round(age_sek / 60)
            og['age'] = age_sek
            og['age_min'] = age_min
            # check if data is older than 15 min and 3 sec, return True/False statement
            if age_sek > (15 * 60 + 3):
                og['old'] = True
            else:
                og['old'] = False
    else:
        og['old'] = True
        og['ts'] = None
        print("No files previous files created at:", log_file)
    return og


if __name__ == "__main__":
    start()
