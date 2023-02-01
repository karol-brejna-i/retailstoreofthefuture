import os

import psycopg

# get the POSTGRES_USR, POSTGRES_PW, POSTGRES_DBdata from environment variables or use default values
POSTGRES_USR = os.getenv('POSTGRES_USR', 'dupaStrongPass')
POSTGRES_PW = os.getenv('POSTGRES_PW', 'dupa-db')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'dupa-db')
POSTGRES_HST = os.getenv('POSTGRES_HST', 'localhost')
DATA_PATH = os.getenv('DATA_PATH', 'data')
# print out the values
print(f"POSTGRES_USR: {POSTGRES_USR}")
print(f"POSTGRES_PW: {POSTGRES_PW}")
print(f"POSTGRES_DB: {POSTGRES_DB}")
print(f"POSTGRES_HST: {POSTGRES_HST}")

PRODUCT_INFO_TABLE_DDL = \
    """
CREATE TABLE product_info (
    product_id INT,
    name VARCHAR(256),
    category VARCHAR(50),
    sizes VARCHAR(50),
    vendor VARCHAR(50),
    description VARCHAR(256),
    buy_price REAL,
    department VARCHAR(10),
    PRIMARY KEY (product_id)
);
"""

COUPON_INFO_TABLE_DDL = \
    """
CREATE TABLE coupon_info (
  coupon_id INT,
  coupon_type REAL,
  department VARCHAR(10),
  discount INT,
  how_many_products_required INT,
  product_mean_price REAL,
  products_available INT,
  start_date VARCHAR(10),
  end_date VARCHAR(10),
  PRIMARY KEY (coupon_id)
);
"""

COUPON_PRODUCT_TABLE_DDL = \
    """
CREATE TABLE coupon_product (
    coupon_id INT,
    product_id INT,
    FOREIGN KEY (coupon_id) REFERENCES coupon_info(coupon_id),
    FOREIGN KEY (product_id) REFERENCES product_info(product_id)
);
"""

CUSTOMER_INFO_TABLE_DDL = \
    """
CREATE TABLE customer_info (
  customer_id INT,
  gender VARCHAR(1),
  age INT,
  mean_buy_price REAL,
  unique_products_bought INT,
  unique_products_bought_with_coupons INT,
  total_items_bought INT,
  mean_discount_received REAL,
  total_coupons_used INT,
  PRIMARY KEY (customer_id)
);
"""

LOAD_DATA_SQL = \
    f"""
COPY coupon_info FROM '{DATA_PATH}/coupon_info.csv' DELIMITER ',' CSV HEADER;
COPY product_info FROM '{DATA_PATH}/products.csv' DELIMITER ',' CSV HEADER;
COPY coupon_product FROM '{DATA_PATH}/coupon_product.csv' DELIMITER ',' CSV HEADER;
COPY customer_info FROM '{DATA_PATH}/customer_info.csv' DELIMITER ',' CSV HEADER;
"""


def get_connection(db, user, password, host):
    return psycopg.connect(
        dbname=db,
        user=user,
        password=password,
        host=host,
        autocommit=True
    )


def create_table(table_name):
    pass


def drop_all(conn):
    cur = conn.cursor()
    cur.execute(f'DROP DATABASE IF EXISTS "{POSTGRES_DB}"')
    # cur.execute(f'DROP USER IF EXISTS "{POSTGRES_USR}"')
    cur.close()


def create_user(conn):
    cur = conn.cursor()
    cur.execute(f'CREATE USER "{POSTGRES_USR}" WITH PASSWORD \'{POSTGRES_PW}\'')
    cur.close()


def create_database(conn):
    cur = conn.cursor()
    cur.execute(f'CREATE DATABASE "{POSTGRES_DB}"')
    cur.close()


def create_tables(conn):
    cur = conn.cursor()
    cur.execute(PRODUCT_INFO_TABLE_DDL)
    cur.execute(COUPON_INFO_TABLE_DDL)
    cur.execute(COUPON_PRODUCT_TABLE_DDL)
    cur.execute(CUSTOMER_INFO_TABLE_DDL)
    cur.close()


def load_data(conn):
    cur = conn.cursor()
    cur.execute(LOAD_DATA_SQL)
    cur.close()


if __name__ == '__main__':
    # connect to the default database
    db_connection = get_connection('postgres', POSTGRES_USR, POSTGRES_PW, POSTGRES_HST)
    print("Connection established")
    drop_all(db_connection)
    # create_user(conn)
    # print("User created")
    create_database(db_connection)
    print("Database created")

    # connect to the database
    db_connection = get_connection(POSTGRES_DB, POSTGRES_USR, POSTGRES_PW, POSTGRES_HST)
    create_tables(db_connection)
    print("Tables created")
    load_data(db_connection)
    print("Data loaded")
    db_connection.close()
