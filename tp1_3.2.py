import psycopg2

tables = '''
    CREATE TABLE IF NOT EXISTS Product(
        asin TEXT PRIMARY KEY NOT NULL,
        title TEXT,
        product_group TEXT,
        salesrank INTEGER
    );
    CREATE TABLE IF NOT EXISTS Similar_(
        product_asin TEXT NOT NULL,
        similar_asin TEXT NOT NULL,
        PRIMARY KEY(product_asin, similar_asin),
        FOREIGN KEY(product_asin) REFERENCES Product(asin)
    );
    CREATE TABLE IF NOT EXISTS Reviews(
        id SERIAL PRIMARY KEY NOT NULL,
        product_asin TEXT NOT NULL, 
        date DATE NOT NULL,
        id_client TEXT NOT NULL,
        rating INTEGER,
        votes INTEGER,
        helpful INTEGER,
        FOREIGN KEY(product_asin) REFERENCES Product(asin)
    );
    CREATE TABLE IF NOT EXISTS Category(
        id INTEGER PRIMARY KEY NOT NULL,
        name TEXT
    );
    CREATE TABLE IF NOT EXISTS Products_per_category(
        product_asin TEXT NOT NULL, 
        category_id INTEGER NOT NULL,
        PRIMARY KEY(product_asin, category_id),
        FOREIGN KEY(product_asin) REFERENCES Product(asin),
        FOREIGN KEY(category_id) REFERENCES Category(id)
    );
'''


def execute():
    cursor = None
    connection = None
    try:
        connection = psycopg2.connect(
            host="localhost",
            user="malbolge",
            password="123456",
            database="postgres",
            port="5432"
        )

        print("Creating tables... ")
        cursor = connection.cursor()
        cursor.execute(tables)
        connection.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgresSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgresSQL connection is closed")


if __name__ == '__main__':
    execute()
