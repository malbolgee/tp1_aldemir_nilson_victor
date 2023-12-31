import sys
import time
from datetime import timedelta
from typing import List

import psycopg2
from tabulate import tabulate


def show_results(option: str, headers: List[str]) -> str:
    return f"[results for option {option}]:\n" + tabulate(cursor.fetchall(), headers, tablefmt='psql') + "\n"


def top_ten_comments() -> str:
    """ Dado um produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com
    menor avaliação. """

    cursor.execute(
        '''
            SELECT title,
                   asin, date, id_client,
                               rating,
                               votes,
                               helpful
            FROM product
            JOIN (
                    (SELECT *
                     FROM Reviews
                     WHERE product_asin = '0231118597'
                     ORDER BY rating DESC, helpful DESC
                     LIMIT 5)
                  UNION ALL
                    (SELECT *
                     FROM Reviews
                     WHERE product_asin = '0231118597'
                     ORDER BY rating ASC, helpful DESC
                     LIMIT 5)) AS X ON asin = product_asin;
        '''
    )
    return show_results("a", ["TITLE", "ASIN", "DATE", "CUSTOMER", "RATING", "VOTES", "HELPFUL"])


def best_similar() -> str:
    """ Dado um produto, listar os produtos similares com maiores vendas do que ele. """

    cursor.execute(
        '''
            SELECT asin,
                   title,
                   product_group,
                   salesrank
            FROM product
            JOIN
              (SELECT similar_asin
               FROM similar_
               WHERE product_asin = 'B00000AU3R') AS p_similar ON asin = similar_asin
            WHERE salesrank >
                (SELECT salesrank
                 FROM product
                 WHERE asin = 'B00000AU3R')
            ORDER BY salesrank DESC;
        '''
    )
    return show_results("b", ["ASIN", "TITLE", "GROUP", "SALESRANK"])


def show_daily_evolution() -> str:
    """ Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no
    arquivo de entrada. """

    cursor.execute(
        '''
            SELECT date, cumulative_avg_rating AS avg_to_date
            FROM
              (SELECT id,
                      product_asin, date, rating,
                                          AVG(rating) OVER (
                                                            ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_avg_rating
               FROM Reviews
               WHERE product_asin = 'B000065U6P') AS avg_rating_to_review
            WHERE avg_rating_to_review.id IN
                (SELECT MAX(id) AS max_id
                 FROM reviews
                 WHERE product_asin = 'B000065U6P'
                 GROUP BY date) -- AS latest_day_review
        '''
    )
    return show_results("c", ["DATE", "AVG_RATING"])


def sales_leaders_by_groups() -> str:
    """ Listar os 10 produtos líderes de venda em cada grupo de produtos. """

    cursor.execute(
        '''
            SELECT *
            FROM
              (SELECT *,
                      RANK() OVER (PARTITION BY product_group
                                   ORDER BY salesrank DESC)
               FROM product) AS R
            WHERE rank <= 10
              AND product_group IS NOT NULL;
        '''
    )
    return show_results("d", ["ASIN", "TITLE", "GROUP", "SALESRANK", "RANK"])


def best_evaluated() -> str:
    """ Listar os 10 produtos com a maior média de avaliações úteis positivas por produto. """

    cursor.execute(
        '''
            SELECT asin,
                   title,
                   product_group,
                   helpful
            FROM product
            JOIN
              (SELECT product_asin,
                      AVG(rating) rating,
                      ROUND(AVG(helpful), 2) helpful
               FROM reviews
               WHERE (votes > 0
                      AND ((helpful * 100) / votes) > 50
                      AND rating > 2)
               GROUP BY product_asin
               ORDER BY helpful DESC
               LIMIT 10) AS R ON product.asin = R.product_asin;
        '''
    )
    return show_results("e", ["ASIN", "TITLE", "GROUP", "HELPFUL"])


def top_category_by_product() -> str:
    """ Listar as 5 categorias de produto com a maior média de avaliações úteis positivas por produto. """

    cursor.execute(
        '''
            SELECT name AS category_name,
                   avg_helpful
            FROM
              (SELECT name,
                      ROUND(AVG(helpful), 2) AS avg_helpful
               FROM category
               INNER JOIN products_per_category ON id = category_id
               INNER JOIN reviews ON reviews.product_asin = products_per_category.product_asin
               WHERE (votes > 0
                      AND ((helpful * 100) / votes) > 50
                      AND rating > 2)
               GROUP BY name
               ORDER BY avg_helpful DESC
               LIMIT 5) AS X
        '''
    )
    return show_results("f", ["CATEGORY_NAME", "AVG_HELPFUL"])


def execute_queries():
    queries_list = [top_ten_comments, best_similar, show_daily_evolution, sales_leaders_by_groups, best_evaluated,
                    top_category_by_product]

    start_time = time.monotonic()
    with open("./output.txt", "w") as out:
        for function in queries_list:
            print(function(), file=out)
        print(f"It took {timedelta(seconds=time.monotonic() - start_time)} seconds to complete the queries", file=out)


def close():
    if connection:
        cursor.close()
        connection.close()
        print("PostgresSQL connection is closed\n")
    sys.exit(0)


if __name__ == "__main__":
    try:
        connection = psycopg2.connect(
            host="localhost",
            user="malbolge",
            password="123456",
            database="postgres",
            port="5432"
        )
        cursor = connection.cursor()

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgresSQL\n", error)
        sys.exit(0)

    try:
        execute_queries()
    except (Exception, psycopg2.Error) as error:
        print("Error while doing queries\n", error)
        sys.exit(0)
