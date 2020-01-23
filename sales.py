import csv
import psycopg2 as pg2


class sales:

    def __init__(self):

        secret='admin'
        self.conn= pg2.connect(database='sales', user='postgres', password=secret)
        self.cur= self.conn.cursor()

###############################################################

    def create_tables(self):
        
        tables=""" CREATE TABLE IF NOT EXISTS temp( 
            region varchar(40), 
            country varchar(40), 
            item_type varchar(40), 
            sales_channel varchar(20), 
            order_priority varchar(20), 
            order_date varchar(40), 
            order_id integer, 
            ship_date date, 
            units_sold integer, 
            unit_price float, 
            unit_cost float, 
            total_revenue float, 
            total_cost float, 
            total_profit float);
            
            CREATE TABLE IF NOT EXISTS regions(
            id serial NOT NULL PRIMARY KEY,
            region varchar(40) UNIQUE);
            
            CREATE TABLE IF NOT EXISTS countries(
            id serial NOT NULL PRIMARY KEY,
            country varchar(40) UNIQUE);
            
            CREATE TABLE IF NOT EXISTS item_types(
            id serial NOT NULL PRIMARY KEY,
            item_type varchar(40) UNIQUE);
            
            CREATE TABLE IF NOT EXISTS sales(
            id serial NOT NULL PRIMARY KEY,
            region_id INTEGER REFERENCES regions(id),
            country_id INTEGER REFERENCES countries(id),
            item_type_id INTEGER REFERENCES item_types(id),
            sales_channel varchar(20),
            order_priority varchar(20),
            order_date varchar(40),
            order_id integer,
            ship_date date,
            units_sold integer,
            unit_price float,
            unit_cost float,
            total_revenue float,
            total_cost float,
            total_profit float,
            UNIQUE (region_id,country_id,item_type_id,sales_channel, order_priority, order_date, order_id, ship_date, units_sold, unit_price, unit_cost, total_revenue, total_cost, total_profit)
            );"""

        self.cur.execute(tables)
        self.conn.commit()

######################################################

    def csv_to_temp(self):
        self.cur.execute("TRUNCATE temp;")
        
        self.conn.commit()
        with open(r"C:\Users\Noha\Desktop\SQL & Python project\100 Sales Records.csv", 'r') as f:
            
            f.readline() #to skip the first line that has the fields names
            self.cur.copy_from(f, 'temp', sep=',')
            self.conn.commit()
#######################################################

    def update_data(self):
        
        moveData=""" 
        INSERT INTO regions(region)
            (
            SELECT region
            FROM temp 
            )
            ON CONFLICT (region) 
            DO NOTHING;

            
            INSERT INTO countries(country)
            (
            SELECT country
            FROM temp 
            )
            ON CONFLICT (country) 
            DO NOTHING;

            INSERT INTO item_types(item_type)
            (
            SELECT item_type
            FROM temp 
            )
            ON CONFLICT (item_type) 
            DO NOTHING;
            
            
            INSERT INTO sales (region_id,country_id,item_type_id,sales_channel, order_priority, order_date, order_id, ship_date, units_sold, unit_price, unit_cost, total_revenue, total_cost, total_profit)
        
            (SELECT regions.id,countries.id,item_types.id,sales_channel, order_priority, order_date, order_id, ship_date, units_sold, unit_price, unit_cost, total_revenue, total_cost, total_profit
            FROM temp
            LEFT JOIN regions ON temp.region=regions.region
            LEFT JOIN countries ON temp.country=countries.country
            LEFT JOIN item_types ON temp.item_type=item_types.item_type)

            ON CONFLICT (region_id,country_id,item_type_id,sales_channel, order_priority, order_date, order_id, ship_date, units_sold, unit_price, unit_cost, total_revenue, total_cost, total_profit)
            DO NOTHING;
            """

        self.cur.execute(moveData)
        self.conn.commit()
