import click
import pandas as pd
import psycopg2

@click.group()
def cli():
    pass

############################

def custom_method():
    return True





############################

database = 'postgres'
user = 'postgres'
password = 'kelly'

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
  print(df)
  df = df.reset_index()  

  #for index, row in df.iterrows():
  #  print(row["index"])

if __name__ == '__main__':
	cli()