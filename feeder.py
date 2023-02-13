import secret
# write a text file for Conky to cat


class write_file:
    def __init__(self, instructions, table) -> None:
        self.instructions = instructions
        self.database = self.database
        self.column = self.column
        self.table = table


    def weather(self):
        database = "website"
        column = "temperature"
        table = ""
        self.database = "website"
        self.column = weather

    def fetcher(self):
        h, u, p = secret.sql()
        db = pymysql.connect(host=h, user=u, passwd=p, db=db_name)
        cursor = db.cursor()
        tmp_data = ""

        sql_query = get_tablestring(table, column)

        try:
            cursor.execute(sql_query)
            tmp_data = cursor.fetchone()

        except pymysql.Error as e:
            print(t_stamp, "Error reading DB: %d: %s" % (e.args[0], e.args[1]))
            print("notingness")

        cursor.close()

        if tmp_data:
            writefile(filename, tmp_data[0])

        else:
            print(t_stamp, "found no values")
            pass

    def get_tablestring(self, column):
        if table == "currency_rate":
            tablestring = "SELECT Rate, Currency, time FROM currency_rate WHERE Currency='SEK' ORDER BY time DESC LIMIT 1"
            return tablestring

        if table == "Bitcoin":
            tablestring = "SELECT {} FROM {} ORDER BY Time DESC LIMIT 1".format(column, table)
            return tablestring

        else:
            tablestring = "SELECT {} FROM {} ORDER BY value_id DESC LIMIT 1".format(column, table)
            return tablestring

    def writefile(self, data):
        with open(secret.file_path() + self.filename + ".txt", "w") as f:
            f.write(self.msg)








if __name__ == "__main__":
    start()
