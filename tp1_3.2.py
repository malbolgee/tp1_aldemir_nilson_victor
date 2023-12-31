import psycopg2

import re

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

LINE_HEADER_REGEX = re.compile(r'^(\w+):\s*(.+)$')
CATEGORY_CONTENT_REGEX = re.compile(r'^(.*)\[(\d+?)]$')
REVIEWS_CONTENT_REGEX = re.compile(r'^(\d{4})-(\d{1,2})-(\d{1,2}) \w+: (\w+) \w+: (\w+) \w+: (\w+) \w+: (\w+)$')


products = []
categories = set()
similar = set()
reviews = []
products_by_categories = set()

YEAR = 1
MONTH = 2
DAY = 3
CUSTOMER_ID = 4
RATING = 5
VOTES = 6
HELPFUL = 7


def normalize(line: str):
    return ' '.join(line.split()).replace('\n', '').strip()


def read_data_from_file(file_name: str):
    attr = ''
    product = {}

    with open(file_name) as f:
        for line in f.readlines():
            line = normalize(line)

            if not line and product:
                products.append(product)
                product = {}

            content_match = LINE_HEADER_REGEX.match(line)

            if content_match and len(content_match.groups()) == 2:
                attr, value = content_match.groups()
                if attr == "similar":
                    similar.update([(product["asin"], attr_similar) for attr_similar in value.split(' ')[1:]])
                elif attr not in ["categories", "reviews", "Id"]:
                    product[attr.lower()] = value
            elif attr == "categories":
                for attr_category in line.split("|")[1:]:
                    name, _id = CATEGORY_CONTENT_REGEX.match(attr_category).groups()
                    products_by_categories.add((product["asin"], _id))
                    categories.add((_id, name))
            elif attr == "reviews":
                reviews_match = REVIEWS_CONTENT_REGEX.match(line)
                if reviews_match:
                    year, month, day = reviews_match.group(YEAR, MONTH, DAY)
                    reviews.append((
                        product["asin"],
                        f"{year}-{month.rjust(2, '0')}-{day.rjust(2, '0')}",
                        reviews_match.group(CUSTOMER_ID),
                        reviews_match.group(RATING),
                        reviews_match.group(VOTES),
                        reviews_match.group(HELPFUL)
                    ))


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

        print("Creating tables...")
        cursor = connection.cursor()
        cursor.execute(tables)
        connection.commit()
        print("Done creating tables")

        print("Reading file...")
        read_data_from_file("./amazon-meta.txt")
        print("Done reading file")

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgresSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgresSQL connection is closed")


if __name__ == '__main__':
    execute()
