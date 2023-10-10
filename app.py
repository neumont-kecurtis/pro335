import click
import pandas as pd
import psycopg2

@click.group()
def cli():
    pass

############################

def get_create_taxi_data_table_sql():
    sql = """
        drop table if exists taxi_data;
        create table if not exists taxi_data
        (
            index                 integer          not null,
            vendorid              integer          not null,
            tpep_pickup_datetime  timestamp        not null,
            tpep_dropoff_datetime timestamp        not null,
            passenger_count       integer          not null,
            trip_distance         double precision not null,
            ratecodeid            integer          not null,
            store_and_fwd_flag    varchar(10)      not null,
            pulocationid          integer          not null,
            dolocationid          integer          not null,
            payment_type          integer          not null,
            fare_amount           double precision not null,
            extra                 double precision not null,
            mta_tax               double precision not null,
            tip_amount            double precision not null,
            tolls_amount          double precision not null,
            improvement_surcharge double precision not null,
            total_amount          double precision not null,
            congestion_surcharge  double precision,
            airport_fee           double precision
        );
    """
    return sql



def get_row_insert(row):
    sql = f"""insert into taxi_data (
                index                    ,
                VendorID                 ,
                tpep_pickup_datetime     ,
                tpep_dropoff_datetime    ,
                passenger_count          ,
                trip_distance            ,
                RatecodeID               ,
                store_and_fwd_flag       ,
                PULocationID             ,
                DOLocationID             ,
                payment_type             ,
                fare_amount              ,
                extra                    ,
                mta_tax                  ,
                tip_amount               ,
                tolls_amount             ,
                improvement_surcharge    ,
                total_amount             ,
                congestion_surcharge     ,
                airport_fee              
                    )
            values (
                {row['index']},  
                {row['VendorID']},  
                '{row['tpep_pickup_datetime']}',  
                '{row['tpep_dropoff_datetime']}',  
                {row['passenger_count']},  
                {row['trip_distance']},  
                {row['RatecodeID']}, 
                '{row['store_and_fwd_flag']}',  
                {row['PULocationID']},  
                {row['DOLocationID']},  
                {row['payment_type']},  
                {row['fare_amount']},  
                {row['extra']},  
                {row['mta_tax']},  
                {row['tip_amount']},  
                {row['tolls_amount']},  
                {row['improvement_surcharge']},  
                {row['total_amount']},  
                {row['congestion_surcharge'] if row['congestion_surcharge'] == "None" else 'null'}, 
                {row['airport_fee'] if row['airport_fee'] == "None" else 'null'}          
            );
            """
    return sql

############################

database = 'postgres'
user = 'postgres'
password = 'kelly'

@cli.command()
@click.option("--file", default='yellow_tripdata_2018-04.parquet')
def pipe_to_pgsql(file):
    print('|reading parquet...')
    df = pd.read_parquet(file, engine='pyarrow')
    df = df.reset_index()  
    print('_reading complete')
        
    try:
        connection = psycopg2.connect(database=database, user=user, password=password)
        cursor = connection.cursor()

        idx = 0
        for index, row in df.iterrows():

            sql = get_row_insert(row)            
            cursor.execute(sql)
            connection.commit()
            idx += 1

            #if idx == 1000000:
            #    break

    except psycopg2.Error as e:
        error = e.pgcode
        print(error)
    finally:
        cursor.close()
        connection.close()


@cli.command()
def pgsql_create_taxi_table():
    connection = psycopg2.connect(database=database, user=user, password=password)
    cursor = connection.cursor()
    try:
        cursor = connection.cursor()
        cursor.execute(get_create_taxi_data_table_sql())
        connection.commit()
        print('create table taxi_data')
    except psycopg2.Error as e:
        error = e.pgcode
        print(error)
    finally:
        cursor.close()
        connection.close()    


@cli.command()
def pgsql_test():
    connection = psycopg2.connect(database=database, user=user, password=password)
    cursor = connection.cursor()
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT VERSION();")
        version = cursor.fetchone()
        print(version)
    except psycopg2.Error as e:
        error = e.pgcode
        print(error)
    finally:
        cursor.close()
        connection.close()

@cli.command()
def pgsql_create_table():
    connection = psycopg2.connect(database=database, user=user, password=password)
    cursor = connection.cursor()
    try:
        cursor = connection.cursor()
        cursor.execute("drop table if exists test_table;")
        cursor.execute("create table if not exists test_table(id bigserial, name varchar(500));")
        connection.commit()
        print('create table [test_table]')
    except psycopg2.Error as e:
        error = e.pgcode
        print(error)
    finally:
        cursor.close()
        connection.close()

@cli.command()
@click.option("--name", default="AWESOMEO-PRO335")
def pgsql_insert_test_record(name):
    connection = psycopg2.connect(database=database, user=user, password=password)
    cursor = connection.cursor()
    try:
        cursor = connection.cursor()
        cursor.execute(f"insert into test_table(name) values ('{name}');")
        connection.commit()
        print('inserted record into [test_table]')
    except psycopg2.Error as e:
        print(f"error:{e}")
    finally:
        cursor.close()
        connection.close()

@cli.command()
@click.option("--file", default='yellow_tripdata_2018-04.parquet')
def process(file):
    df = pd.read_parquet(file, engine='pyarrow')
    df = df.reset_index()  

    idx = 0
    for index, row in df.iterrows():
        print(row)
        
        idx += 1
        if idx == 1000:
            break

if __name__ == '__main__':
	cli()