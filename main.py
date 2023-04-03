import func
import users
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

conn = func.connect_to_psql(users.param_dic_lyzin)
sbt_vehicle_list = func.read_psql_vehicle_list(conn)
vehicles = sbt_vehicle_list['sbt_vehicle_id'].unique()
veh_qty = len(vehicles)
counter = 0

for i in vehicles:
    print('we have started with vehicle #' + str(i))
    func.sub_table_creation(conn, i)
    df = func.read_psql_vehicle_records(conn)
    vehicle_daily_mileage, vehicle_daily_mileage_dirty = func.sbt_vehicle_mileage_calculation(df)
    func.execute_values(conn, vehicle_daily_mileage, 'pbi.telematics_sbt_daily_mileage_clean')
    func.execute_values(conn, vehicle_daily_mileage_dirty, 'pbi.telematics_sbt_daily_mileage_dirty')
    counter+=1
    complete = round(counter / veh_qty * 100, 2)
    print(str(complete) + "% is completed")

