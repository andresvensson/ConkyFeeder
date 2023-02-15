import pymysql
import secret


# write a text file for Conky to cat


class Get_Data:
    """Get values from my database.
    Construct a nice print.
     Parse into two files.
    Conky app will run concatenate (cat) those to the files"""

    def __init__(self, request: str) -> None:
        self.data = {}
        self.db_name = None
        self.table = None
        self.column = None
        self.msg = None
        self.weather()
        # if request == "weather":
        #    self.weather()

    def weather(self):
        d = {}
        self.db_name = "website"
        self.column = "temperature"
        tn = "weather_"
        self.table = tn + "datarum"
        d['datarum'] = self.fetcher()[0]
        self.table = tn + "sovrum"
        d['sovrum'] = self.fetcher()[0]
        self.table = tn + "kitchen"
        d['kitchen'] = self.fetcher()[0]

        # return msg
        self.table = tn + "outside"
        d['outside'] = self.fetcher()
        self.column = "*"
        d['all'] = self.fetcher()

        d['tot_entires'] = d['all'][0]
        d['ts'] = d['all'][1]
        d['outside'] = d['all'][3]
        d['wind_speed'] = d['all'][9]
        d['wind_deg'] = d['all'][10]
        d['cloud'] = d['all'][12]
        d['sunrise'] = d['all'][13]
        d['sunset'] = d['all'][14]
        d['status'] = d['all'][15]
        for x in d:
            print(x, ":", d[x])
        #print(d)
        pass

    def fetcher(self):
        h, u, p = secret.sql()
        db = pymysql.connect(host=h, user=u, passwd=p, db=self.db_name)
        c = db.cursor()
        sql_data = None

        try:
            c.execute(self.sql_query())
            sql_data = c.fetchone()
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
