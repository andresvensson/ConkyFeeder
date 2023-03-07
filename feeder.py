import pymysql
import secret as s


# write a text file for Conky to cat


class Get_Data:
    """Get values from my database.
    Construct a nice print.
     Parse into two files.
    Conky app will run concatenate (cat) the files"""

    def __init__(self, request: str) -> None:
        self.data = {}
        self.db_name = None
        self.table = None
        self.column = None
        self.msg = None
        self.data = self.collect_data()
        self.parse_data()

    def parse_data(self):
        print("data:\nKey : Value : Datatype\n")
        for x in self.data:
            print(x, ":", self.data[x], ":", type(self.data[x]))

    def collect_data(self):
        d = {}
        self.db_name = s.db_name1()
        self.column = s.column1()
        tn = "weather_"
        self.table = tn + s.table1()
        d['datarum'] = self.fetcher()[0]
        self.table = tn + s.table2()
        d['sovrum'] = self.fetcher()[0]
        self.table = tn + s.table3()
        d['kitchen'] = self.fetcher()[0]

        # return msg
        self.table = tn + s.table4()
        d['outside'] = self.fetcher()
        self.column = "*"
        all_w = self.fetcher()

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

        self.table = s.table5()
        d['euro'] = self.fetcher()[0]

        self.db_name = s.db_name2()
        self.table = s.table6()
        d['btc'] = self.fetcher()[0]

        return d

    def fetcher(self):
        h, u, p = s.sql()
        db = pymysql.connect(host=h, user=u, passwd=p, db=self.db_name)
        c = db.cursor()
        sql_data = None

        try:
            c.execute(self.sql_query())
            sql_data = c.fetchone()
            print("Raw data:", sql_data)
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
        if self.table == "currency_rate":
            return "SELECT Rate, Currency, time FROM currency_rate WHERE Currency='SEK' ORDER BY time DESC LIMIT 1"

        if self.table == "Bitcoin":
            return "SELECT {} FROM {} ORDER BY Time DESC LIMIT 1".format(self.column, self.table)

        else:
            return "SELECT {} FROM {} ORDER BY value_id DESC LIMIT 1".format(self.column, self.table)

    def print_data(self):
        return self.data


def writefile(self):
    with open("conky_assets/" + self.table + ".txt", "w") as f:
        f.write(str(self.msg))


# class Rates(Get_Data):
#    def __init__(self):
#        super().__init__(self, instructions, database, column, table)
#        print("subclass")
#        database = "website"
#        column = "temperature"
#        table = ""
#        sql_query = "SELECT {} FROM {} ORDER BY value_id DESC LIMIT 1".format(column, table)
#        pass


def start():
    print("call Weather")
    #init = Get_Data("weather")
    Get_Data("weather")
    # TODO
    # Just skip the weather method. Collect all data in one go
    # Parse and render two files
    # Rename class and methods


if __name__ == "__main__":
    start()
