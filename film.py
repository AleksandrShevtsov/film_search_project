import mysql.connector
import os
from tabulate import tabulate


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


def query_dict(query_key) -> str:
    queries = {
        'main_query': '''
                SELECT a.first_name, a.last_name, f.title, f.description, f.release_year FROM film f
                %s
                GROUP BY f.title LIMIT 10
                ''',
        'main_table': 'SELECT actor.first_name, actor.last_name, film.title, film.description, film.release_year '
                      'FROM film %s %s LIMIT 10',
        'actor_table': 'JOIN film_actor fa ON f.film_id=fa.film_id '
                       'JOIN actor a ON fa.actor_id=a.actor_id %s GROUP BY f.title LIMIT 10',
        'category_table': 'JOIN film_category fc ON f.film_id=fc.film_id'
                          'JOIN category c ON fc.category_id=c.category_id %s ',
        'rating_query': 'SELECT rating_list FROM rating',
        'category_query': 'SELECT category_id, name FROM category',
        'actor_query': 'SELECT first_name, last_name FROM actor ORDER BY last_name',
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
    clear_screen()

    # Split the results into 4 columns
    column_count = 4
    row_count = len(results) // column_count + (len(results) % column_count > 0)
    columns = [results[i * row_count:(i + 1) * row_count] for i in range(column_count)]

    # Format the columns
    tables = []
    headers = ['№', 'Жанр']
    for col in columns:
        tables.append(tabulate(col, headers=headers, tablefmt='rounded_grid'))

    # Print the columns side by side
    max_lines = max(len(table.split('\n')) for table in tables)
    table_lines = [table.split('\n') for table in tables]

    for i in range(max_lines):
        for table in table_lines:
            if i < len(table):
                print(table[i].ljust(20), end=' ')
            else:
                print(' ' * 30, end=' ')
        print()

    cursor.close()
    disconnect(connection)
    return results


def create_table(table):
    table_list = {
        'main_table': [['1', 'Название фильма'], ['2', 'Имя актёра'], ['3', 'Жанр\nГод выпуска'],
                       ['4', 'Показать 5 самых частых запросов'], ['0', 'Выход']],
        'category_year_table': [['1', 'Жанр'], ['2', 'Год выпуска'], ['3', 'Жанр и\nГод выпуска'], ['0', 'Выход']],
    }
    clear_screen()
    table = table_list.get(table)
    print(tabulate(table, tablefmt='rounded_grid'))


def show_top_queries():
    top_queries = get_top_queries()
    clear_screen()
    connection = Sakila_connect()
    cursor = connection.cursor()
    query_list = []
    print("Самые частые запросы:")
    for query_filter, _ in top_queries:
        query = query_dict('main_table') % (query_dict('actor_table'), query_filter)
        cursor.execute(query)
        results = cursor.fetchall()
        query_list += results
    print(tabulate(query_list, headers=['Название фильма', 'Описание', 'Год выпуска'], tablefmt='rounded_grid'))
    input("\nНажмите Enter, чтобы вернуться в главное меню...")
    cursor.close()
    disconnect(connection)


def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')


def get_top_queries() -> list:
    connection = MyDb_connect()
    cursor = connection.cursor()
    query = 'SELECT rating_list, COUNT(*) as count FROM rating GROUP BY rating_list ORDER BY count DESC LIMIT 5'
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    disconnect(connection)
    return results


def filter_for_query(choice) -> str:
    filters = []
    if '1' in choice:
        title = input('Введите название фильма: ').upper()
        filters.append(f"f.title LIKE '%{title}%'")
    if '2' in choice:
        actor_first_name = input('Введите имя актёра: ').upper()
        if actor_first_name != '':
            actor_first_name = f"a.first_name LIKE '%{actor_first_name}%'"
        actor_last_name = input('Введите фамилию актёра: ').upper()
        if actor_last_name.lower() != '':
            actor_last_name = f"a.last_name LIKE '%{actor_last_name}%'"
        if actor_last_name != '' or actor_first_name != '':
            if actor_first_name != '' and actor_last_name != '':
                actor = ' AND '.join([actor_first_name, actor_last_name])
            elif actor_first_name != '':
                actor = actor_first_name
            else:
                actor = actor_last_name
            filters.append(f"{actor}")
        else:
            clear_screen()
            _query = query_dict('actor_query')
            connection = Sakila_connect()
            cursor = connection.cursor()
            cursor.execute(_query)
            results = cursor.fetchall()
            cursor.close()
            disconnect(connection)
            print(tabulate(results, headers=['Имя', 'Фамилия'], tablefmt='rounded_grid'))
            if input("\nНажмите Enter, чтобы вернуться в главное меню или 0 для выхода...") == '0':
                quit()
            return ''
    
    if '3' in choice:
        print(flush=True)
        create_table('category_year_table')
        choice = input('Выберите один из вариантов цифрами: ')
        if '1' in choice:
            categories = fetch_category_list()
            category_id = input('Выберите один из жанров цифрами: ')
            filters.append(f"fc.category_id = {category_id})")
        if '2' in choice:
            year = input('Введите год выпуска: ')
            filters.append(f"f.release_year = {year}")
        if '3' in choice:
            fetch_category_list()
            category_id = input('Выберите один из жанров цифрами: ')
            if not category_id.isdigit():
                while not category_id.isdigit() or category_id == '':
                    category_id = input('Введите жанр: ')
            filters.append(f"fc.category_id = {int(category_id)})")
            year = input('Введите год выпуска: ')
            if not year.isdigit():
                while year == '' or not year.isdigit():
                    year = input('Введите год выпуска: ')
            filters.append(f"а.release_year = {year}")
    if '4' in choice:
        show_top_queries()
    if '0' in choice:
        quit()
    
    if filters:
        return 'WHERE ' + ' AND '.join(filters)
    return ''


def film_list(query_filter):
    connection = Sakila_connect()
    cursor = connection.cursor()
    query = ((query_dict('main_query') % query_dict('actor_query')) % query_dict('category_query')) % query_filter
    cursor.execute(query)
    results = cursor.fetchall()
    if results == []:
        print('Ничего не найдено')
        return
    clear_screen()
    print(tabulate(results, headers=['Актёр', 'Название фильма', 'Описание', 'Год выпуска'], tablefmt='rounded_grid'))
    print('\nНажмите Enter, чтобы вернуться в главное меню или 0 для выхода...')
    cursor.close()
    disconnect(connection)
    if input() == '0':
        quit()


if __name__ == "__main__":
    while True:
        clear_screen()
        create_table('main_table')
        choice = input('Выберите фильтр поиска фильма: ')
        query_filter = filter_for_query(choice)
        if query_filter:
            insert_query(query_filter)
            film_list(query_filter)
