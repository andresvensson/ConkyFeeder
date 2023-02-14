import pymysql
import secret


# write a text file for Conky to cat


class Get_Data:
    """Get values from my database."""
    def __init__(self, db_name, column, table) -> None:
        self.db_name = db_name
        self.column = column
        self.table = table
        self.data = self.fetcher()

    def fetcher(self):
        h, u, p = secret.sql()
        db = pymysql.connect(host=h, user=u, passwd=p, db=self.db_name)
        c = db.cursor()
        sql_data = None
        #sql_query = get_tablestring(table, column)

        try:
            c.execute(self.sql_query())
            sql_data = c.fetchone()
            c.close()

        except pymysql.Error as e:
            print("Error reading DB: %d: %s" % (e.args[0], e.args[1]))
            print("nothingness")

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
        return self.data[0]


class Write_Data(Get_Data):
    """Callable presets that write a file for Conky my app"""
    def __init__(self, db_name, column, table):
        # super inherit variables from Get_Data
        super().__init__(db_name, column, table)
        print("subclass")

    def weather(self):
        database = "website"
        column = "temperature"
        table = ""
        sql_query = "SELECT {} FROM {} ORDER BY value_id DESC LIMIT 1".format(column, table)

    def construct(self):
        write_dis = "Test"
        self.table = "some table name"
        return write_dis

    def writefile(self):
        with open("conky_assets/" + self.table + ".txt", "w") as f:
            f.write(self.data)


#class Rates(Get_Data):
#    def __init__(self):
#        super().__init__(self, instructions, database, column, table)
#        print("subclass")
#        database = "website"
#        column = "temperature"
#        table = ""
#        sql_query = "SELECT {} FROM {} ORDER BY value_id DESC LIMIT 1".format(column, table)
#        pass


def writefile(self, data):
    with open(secret.file_path() + self.filename + ".txt", "w") as f:
        f.write(self.msg)


def start():
    # make two files
    print("call Weather")
    print("call Rates")
    # test it:
    # database, column, table
    w_data = Get_Data("website", "temperature", "weather_datarum")
    print(w_data.print_data())

    # weather_msg = class.weather()
    # write_file(weather_msg)


if __name__ == "__main__":
    start()
