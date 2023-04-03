import sys
import psycopg2
import pandas as pd
import time
import os
from geopy.distance import distance
import numpy as np
import psycopg2.extras as extras
from io import StringIO
from datetime import date
from sqlalchemy import create_engine
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
import warnings
warnings.filterwarnings("ignore")





def connect_to_psql(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn





def read_psql_vehicle_list(conn):
    sbt_vehicle_list_sql = """
        SELECT obj_id as sbt_vehicle_id,
        model,
        vin,
        gos as plate,
        mpt
        FROM pbi.sbt_all_cars
        WHERE vin is not null
        """
    df = pd.read_sql(sbt_vehicle_list_sql, conn)
    print("list of sbt vehicles is created")
    return df


def sub_table_creation(conn, vehicle_id):
    cur = conn.cursor()
    try:
        cur.execute("""DROP TABLE IF EXISTS pbi.telematics_sbt_sub;
                CREATE TABLE pbi.telematics_sbt_sub AS (SELECT * FROM pbi.sbt_all_records WHERE obj_id = %s);""" %(vehicle_id))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    print("table fro vehicle #" + str(vehicle_id) + " has been created")
    return 1


def read_psql_vehicle_records(conn):
    sbt_sql = """
    SELECT 
    id,
    obj_id,
    AGE(record_date, LAG(record_date,1) OVER (ORDER BY record_date ASC)) as duration_time,
    record_date,
    LAG(record_date,1) OVER (ORDER BY record_date ASC) as prev_record_date,
    lat,
    lon,
    LAG(lat,1) OVER (ORDER BY record_date ASC) as prev_lat,
    LAG(lon,1) OVER (ORDER BY record_date ASC) as prev_lon,
    velocity
    FROM pbi.telematics_sbt_sub
    ORDER BY record_date
    """
    df = pd.read_sql(sbt_sql, conn)
    print("records has been sent to calculation")
    return df


def sbt_vehicle_mileage_calculation(df):
    try:
        # drom empty rows (no prev. coordinates)
        df = df.dropna()

        # prepare coordinates
        df['prev_coordinates'] = df[['prev_lat', 'prev_lon']].apply(tuple, axis=1)
        df['coordinates'] = df[['lat', 'lon']].apply(tuple, axis=1)

        # calculate distance between points
        df['distance_m'] = df.apply(lambda row: distance(row['prev_coordinates'], row['coordinates']).m \
            if row['coordinates'] is not None else float('nan'), axis=1)

        # addind event date
        df['date'] = df['record_date'].dt.date

        # summ of dirty mileage
        vehicle_daily_mileage_dirty = df.groupby(['obj_id', 'date'], as_index=False).agg({'distance_m': 'sum'})
        vehicle_daily_mileage_dirty['path_km'] = vehicle_daily_mileage_dirty['distance_m'] / 1000

        # calculate time in sec
        df['duration_time_sec'] = df['duration_time'] / np.timedelta64(1, 's')

        # calculate speed in kmh
        df['speed_to_cur_time_kmh'] = df['distance_m'] / df['duration_time_sec'] * 3.6

        # remove records with speed > 280 kmh
        df = df[df['speed_to_cur_time_kmh'] < 280]

        # adding prev speed to row
        df['prev_speed'] = df['speed_to_cur_time_kmh'].shift(1)

        # calculating acceleration
        df['acceleration'] = round(
            ((df['speed_to_cur_time_kmh'] / 3.6) - (df['prev_speed'] / 3.6)) / df['duration_time_sec'], 4)

        # clearing df from enormous accelerations
        df_cl = df[df['acceleration'] < 6.94]  # acceleration clean

        df_cl = df_cl[df_cl['acceleration'] > -9]  # deceleration clean

        # groupping_result_by_date
        vehicle_daily_mileage = df_cl.groupby(['obj_id', 'date'], as_index=False).agg({'distance_m': 'sum'})
        vehicle_daily_mileage['path_km'] = vehicle_daily_mileage['distance_m'] / 1000
    except:
        vehicle_daily_mileage = pd.DataFrame()

    print('vehicle is done!')
    return vehicle_daily_mileage, vehicle_daily_mileage_dirty


def execute_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]

    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()