import mysql.connector
from tabulate import tabulate


def MyDb_connect():
    config = {
        'user': 'ich1',
        'password': 'ich1_password_ilovedbs',
        'host': 'mysql.itcareerhub.de',
        'database': 'city',
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


def query_dict(query):
    query_dict = {
        'main_query': 'SELECT title, description, release_year FROM film %s LIMIT 10',
        'category_query': 'SELECT category_id, name FROM category',
    }
    query = query_dict.get(query)
    return query


def category_list():
    connection = Sakila_connect()
    cursor = connection.cursor()
    cursor.execute(query_dict('category_query'))
    results = cursor.fetchall()
    print(tabulate(results, headers=['№', 'Жанр'], tablefmt='grid'))
    cursor.close()
    disconnect(connection)
    return results


def user_choice():
    print('Выберите способ поиска фильма:')
    serch_list = [['1', 'По названию фильма'], ['2', 'По имени актёра'], ['3', 'По году выпуска'], ['4', 'По жанру']]
    print(tabulate(serch_list, tablefmt='grid'))
    
    choice = input('Введите Ваш выбор цифрами через пробел: ').split()
    return choice


def filter_for_query(choice):
    category_list = []
    if len(choice) == 1:
        query = 'WHERE '
    else:
        query = []
    for i in range(len(choice)):
        if choice[i] == '1':
            title = input('Ведите название фильма: ')
            query += 'title = ' + title + ' '
        elif choice[i] == '2':
            name = input('Ведите имя актёра: ')
            query += 'first_name = ' + name + ' '
        elif choice[i] == '3':
            year = input('Ведите год выпуска: ')
            query += 'release_year = ' + year + ' '
        elif choice[i] == '4':
            category_list = category_list()
            category = int(input('Выбирите один из жанров цифрами: '))
            user_choice = category_list[category]
            query += 'WHERE name = ' + user_choice + ' '
    return query

def film_list(query):
    connection = Sakila_connect()
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    print(tabulate(results, headers=['title', 'description', 'release_year'], tablefmt='grid'))
    cursor.close()
    disconnect(connection)
    
    
connection = Sakila_connect()
cursor = connection.cursor()
choice = user_choice()
cursor.close()
disconnect(connection)
my_filter = filter_for_query(choice)
film_list(filter)
