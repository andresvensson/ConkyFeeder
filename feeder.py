#!/usr/bin/env python3
import sys
import time
import logging
import datetime as dt

import pymysql
import os
import secret as s  # your secret/config module

# ------------------- Config -------------------
DEV_MODE = s.dev_mode()
DATA_FILE = s.file_path() + "conky_data.txt"  # single combined output
CACHE_FILE = s.file_path() + "created.txt"

logging.basicConfig(level=logging.INFO, filename="log.log", filemode="w",
                    format="%(asctime)s - %(levelname)s - %(message)s")

# ------------------- Helpers -------------------
def get_db_connection(db_name):
    h, u, p = s.sql()
    return pymysql.connect(host=h, user=u, passwd=p, db=db_name, charset="utf8mb4",
                           cursorclass=pymysql.cursors.DictCursor)


def fetch_sql(db_name, table, column="*", extra_condition="", order_by="id"):
    conn = get_db_connection(db_name)
    result = None
    try:
        with conn.cursor() as cursor:
            query = f"SELECT {column} FROM {table} {extra_condition} ORDER BY {order_by} DESC LIMIT 1"
            if DEV_MODE:
                print("QUERY:", query)
            cursor.execute(query)
            result = cursor.fetchone()
    except pymysql.Error as e:
        logging.exception(f"DB error: {e}")
    finally:
        conn.close()
    return result


# def load_cache():
#     if os.path.isfile(CACHE_FILE):
#         with open(CACHE_FILE, "r") as f:
#             ts = dt.datetime.strptime(f.read().strip(), "%Y-%m-%d %H:%M:%S")
#             age = (dt.datetime.now() - ts).total_seconds()
#             return {"ts": ts, "age": age, "old": age > 15 * 60}
#     return {"ts": None, "age": None, "old": True}


def save_cache(ts):
    with open(CACHE_FILE, "w") as f:
        #f.write(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        f.write(ts.strftime("%Y-%m-%d %H:%M:%S"))


def format_data(data):
    #print("Constructing data textfile..")
    # 0
    lines = ["Local Weather"]

    # align l/r within 54 characters
    def align_lr(left: str, right: str, width: int = 54) -> str:
        left = left[:max(0, width - len(right) - 1)]  # leave space for right and one space
        return f"{left:<{width - len(right)}}{right}"

    # 1
    # scattered clouds, clouds: 40%          Kitchen: 23.4°C
    #d = data['outside']
    d = data.get('outside', 0)
    if d:
        left = f"{d['status']}, clouds: {d.get('clouds', 0)}%"
        #right = f"Kitchen: {data['kitchen']['temperature']}°C"
    else:
        left = ""
    right = f"Kitchen: {data['kitchen'].get('temperature', "XX")}°C"
    lines.append(align_lr(left, right))

    # 2
    # wind: speed: 3.09, deg: 70             Datarum: 23.4°C
    def wind_direction(degrees: float) -> str:
        """Convert wind direction in degrees to short compass abbreviation (e.g., 'ENE')."""
        directions = [
            "N", "NNE", "NE", "ENE",
            "E", "ESE", "SE", "SSE",
            "S", "SSW", "SW", "WSW",
            "W", "WNW", "NW", "NNW",
        ]
        # Normalize degrees between 0–360
        degrees = degrees % 360
        # Each sector = 22.5 degrees
        index = int((degrees + 11.25) // 22.5) % 16
        return directions[index]

    if d:
        wd = d.get('wind_deg', 0)
        left = f"wind {d.get('wind_speed', "XX")} m/s, deg: {wd} ({wind_direction(wd)})"
    else:
        left = ""
    right = f"Datarum: {data['datarum'].get('temperature', "XX")}°C"
    lines.append(align_lr(left, right))


    # 3
    # precipitation?                           Lowes: 24.2°C
    if d['rain_1h']:
        left = f"rain 1h: {d['rain_1h']}mm"
        if d['rain_3h']:
            left += f", 3h: {d['rain_3h']}mm"
    elif d['snow_1h']:
        left = f"snow 1h: {d['snow_1h']}mm"
        if d['snow_3h']:
            left += f", 3h: {d['rain_3h']}mm"
    else:
        left = ""
    right = f"Lowes: {data['sovrum'].get('temperature', "XX")}°C"
    lines.append(align_lr(left, right))

    # 5
    # Humidity, out: 82%, datarum: 54%      Outside: 16.12°C
    left = f"Humidity: Out {d['humidity']}%, datarum {data['datarum']['humidity']}%"
    right = f"Outside: {d['temperature']}°C"
    lines.append(align_lr(left, right))

    # 6
    # Daylight: 5:25 -> 17:55
    lines.append(
        f"Daylight: {str(d['sunrise'] + dt.timedelta(hours=1))[:-3]} -> {str(d['sunset'] + dt.timedelta(hours=1))[:-3]}")

    # 7
    lines.append(" ")

    # 8
    lines.append("Economy")

    # 9
    # 1 EUR = 11.324 SEK / 1.1124 USD
    lines.append(f"1 EUR = {data['sek']['Rate']} SEK / {data['usd']['Rate']} USD")

    # 10
    # 1 BTC = 61 938 USD (2.9 %)
    price = f"{round(data['btc']['Price']):,}".replace(",", " ")
    lines.append(f"1 BTC = {price} USD ({data['btc']['percent_change_24']} %)")

    # 11
    # Gold: 4113 USD/once, Silver: 48 USD/ounce
    lines.append(f"Gold: {int(data['gold']['rate'])} USD/ounce, Silver: {int(data['silver']['rate'])} USD/ounce")

    # 12
    # NordPool: 19.42 öre/kWh
    if data['NordPool']:
        # kr/MWh -> öre/kWh
        # 1 MWh = 1000 kWh, 1 kr = 1 000 öre
        lines.append(f"NordPool: {round(data['NordPool']['value'] / 10, 2)} öre/kWh")
    else:
        lines.append("Has no NordPool stats to show..")

    # 13
    # TS from DB: 2024-09-19 11:00:05, total: 83 191
    tot = f"{data['outside']['value_id']:,}".replace(",", " ")
    lines.append(f"TS from DB: {data['outside']['time_stamp']}, total {tot}")

    # 14
    # Thursday, week 38
    ts = dt.datetime.now()
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    lines.append(f"{days[ts.weekday()]}, week {ts.isocalendar()[1]}")

    return "\n".join(lines)


def write_output(msg):
    with open(DATA_FILE, "w") as f:
        f.write(msg)
    #save_cache()


# ------------------- Main -------------------
def collect_data():
    d = {}

    # Weather
    db_name = "website"
    table = "weather_datarum"
    column = "temperature, humidity"
    extra_condition = ""
    order_by = "value_id"
    d["datarum"] = fetch_sql(db_name, table, column, "", order_by)

    table = "weather_sovrum"
    column = "temperature"
    d["sovrum"] = fetch_sql(db_name, table, column, order_by = "value_id")

    table = "weather_kitchen"
    d["kitchen"] = fetch_sql(db_name, table, column, order_by = "value_id")

    table = "weather_outside"
    d['outside'] = fetch_sql(db_name, table, order_by="value_id")

    # Currency
    table = s.table5()
    d['usd'] = fetch_sql(s.db_name1(), table, extra_condition="WHERE Currency='USD'", order_by="value_id")
    d['sek'] = fetch_sql(s.db_name1(), table, extra_condition="WHERE Currency='SEK'", order_by="value_id")

    # BTC
    table = s.table6()
    d["btc"] = fetch_sql(s.db_name2(), table)

    # NordPool
    table = "NordPool"
    d["NordPool"] = fetch_sql(s.db_name2(), table, extra_condition="WHERE NOW() BETWEEN start AND end", order_by="value_id")

    # Metals from DB: Example
    metals_table = "metal_prices"
    d["gold"] = fetch_sql(s.db_name2(), metals_table, extra_condition="WHERE metal='USDXAU'")
    d["silver"] = fetch_sql(s.db_name2(), metals_table, extra_condition="WHERE metal='USDXAG'")

    if DEV_MODE:
        print("...VALUES DICT...")
        for x in d:
            print(x, ":", d[x])
        print(".................")

    return d


def main():
    while True:
        if os.path.isfile(CACHE_FILE):
            with open(CACHE_FILE, "r") as f:
                ts = dt.datetime.strptime(f.read().strip(), "%Y-%m-%d %H:%M:%S")
                age = (dt.datetime.now() - ts).total_seconds()
                print("Cached data is", int(age / 60), "minutes old.")
        else:
            age = 901

        if age > 15 * 60:
            print("Get data")
            try:
                data = collect_data()
            except Exception as e:
                logging.exception(f"Could not get data. Error:\n{e}")
                continue

            if data:
                msg = format_data(data)
                ts_db = data['outside']['time_stamp']
                age = (dt.datetime.now() - ts_db).total_seconds()
                ttl = (15 * 60) - age
                age = ttl + 2

                save_cache(ts_db)
                print("TTL:", int(ttl / 60), "minutes")
            else:
                msg = (f"Could not get data to parse\n"
                       f"Trying to get new data in 5 minutes "
                       f"({(dt.datetime.now() + dt.timedelta(minutes=5)).strftime("%H:%M")})")
                logging.error("Has no data to parse")
                time.sleep(300)

            write_output(msg)

            if DEV_MODE:
                print(".....TXT FILE.....")
                print(msg)
                print("..................")
                break

        else:
            print(f"Data still fresh, next update in {round((15 * 60 - age) / 60)} min")
            if DEV_MODE:
                print("Delete '../conky_assets/created.txt' to force a re-run")
                break
            time.sleep(10)
        time.sleep(age)






if __name__ == "__main__":
    logging.info("Feeder started")
    main()
