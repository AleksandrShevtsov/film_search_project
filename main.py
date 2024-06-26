import mysql.connector
from tabulate import tabulate
import curses


def MyDb_connect():
    config = {
        'user': 'ich1',
        'password': 'ich1_password_ilovedbs',
        'host': 'mysql.itcareerhub.de',
        'database': 'project_220424ptm_OShevtsov',
        'raise_on_warnings': True
    }
    return mysql.connector.connect(**config)


def Sakila_connect():
    config = {
        'user': 'ich1',
        'password': 'password',
        'host': 'ich-db.ccegls0svc9m.eu-central-1.rds.amazonaws.com',
        'database': 'sakila',
        'raise_on_warnings': True
    }
    return mysql.connector.connect(**config)


def disconnect(connection):
    connection.close()


def query_dict(query_key):
    queries = {
        'main_query': 'SELECT title, description, release_year FROM film %s LIMIT 10',
        'category_query': 'SELECT category_id, name FROM category',
        'actor_query': 'SELECT first_name, last_name FROM actor',
    }
    return queries.get(query_key)

def insert_query(query_key):
    connection = MyDb_connect()
    cursor = connection.cursor()
    query = f'INSERT INTO rating (rating_list) VALUES ("{query_key}")'
    cursor.execute(query)
    connection.commit()
    cursor.close()
    disconnect(connection)


def fetch_category_list():
    connection = Sakila_connect()
    cursor = connection.cursor()
    cursor.execute(query_dict('category_query'))
    results = cursor.fetchall()
    print(tabulate(results[:7], headers=['№', 'Жанр'], tablefmt='grid'))
    cursor.close()
    disconnect(connection)
    return results


def create_table(table):
    table_list = {
        'main_table': [['1', 'Название фильма'], ['2', 'Имя актёра'], ['3', 'Жанр\nГод выпуска']],
        'category_year_table': [['1', 'Жанр'], ['2', 'Год выпуска'], ['3', 'Жанр и\nГод выпуска']],
    }
    table = table_list.get(table)
    print(tabulate(table, tablefmt='grid'))


def filter_for_query(choice):
    filters = []
    if '1' in choice:
        title = input('Введите название фильма: ').upper()
        filters.append(f"title LIKE '%{title}%'")
    if '2' in choice:
        actor_first_name = input('Введите имя актёра: ')
        actor_last_name = input('Введите фамилию актёра: ')
        _query = ('film_id IN (SELECT film_id FROM film_actor WHERE actor_id IN '
                  '(SELECT actor_id FROM actor WHERE first_name LIKE')
        filters.append(
            f"{_query} '%{actor_first_name}%' AND last_name LIKE '%{actor_last_name}%'))")
    if '3' in choice:
        create_table('category_year_table')
        choice = input('Выберите один из выриантов цифрами: ')
        if '1' in choice:
            categories = fetch_category_list()
            category_id = int(input('Выберите один из жанров цифрами: '))
            filters.append(f"film_id IN (SELECT film_id FROM film_category WHERE category_id = {category_id})")
        if '2' in choice:
            year = input('Введите год выпуска: ')
            filters.append(f"release_year = {year}")
        if '3' in choice:
            pass
    if filters:
        return 'WHERE ' + ' AND '.join(filters)
    return ''
    



def film_list(query_filter):
    connection = Sakila_connect()
    cursor = connection.cursor()
    query = query_dict('main_query') % query_filter
    cursor.execute(query)
    results = cursor.fetchall()
    print(tabulate(results, headers=['title', 'description', 'release_year'], tablefmt='grid'))
    cursor.close()
    disconnect(connection)


# Main execution
if __name__ == "__main__":
    create_table('main_table')
    choice = input('Выберите фильтр поиска фильма:')
    query_filter = filter_for_query(choice)
    insert_query(query_filter)
    film_list(query_filter)
