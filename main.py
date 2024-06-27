import mysql.connector
import os
from tabulate import tabulate


def mydb_connect():
    """Подключение к первой базе данных MySQL"""
    config = {
        'user': 'ich1',
        'password': 'ich1_password_ilovedbs',
        'host': 'mysql.itcareerhub.de',
        'database': 'project_220424ptm_OShevtsov',
        'raise_on_warnings': True
    }
    return mysql.connector.connect(**config)


def ich_connect():
    """Подключение ко второй базе данных MySQL"""
    config = {
        'user': 'ich1',
        'password': 'password',
        'host': 'ich-db.ccegls0svc9m.eu-central-1.rds.amazonaws.com',
        'database': 'sakila',
        'raise_on_warnings': True
    }
    return mysql.connector.connect(**config)


def disconnect(connection):
    """Закрытие подключения к базе данных"""
    connection.close()


# Переводит SQL-запросы на понятный язык и заменяет  поля на названия понятные для пользователя
def translate_queries(queries, category_dict):
    translations = {
        'fc.category_id': 'Жанр',
        'f.title': 'название',
        'a.first_name': 'имя актера',
        'a.last_name': 'фамилия актера',
        'f.release_year': 'год выпуска'
    }
    
    def clean_query(query):
        query = query.replace('WHERE', '').replace('LIKE', '').replace(',', '').replace('%', '').strip()
        # Разбиваем запрос на части по "AND"
        parts = query.split(' AND ')
        
        translated_parts = []
        
        for part in parts:
            for key, value in translations.items():
                if str(key) in part:
                    part = part.replace(str(key), value)
                    if 'Жанр' in part:
                        part = part.split()
                        print(category_dict)
                        part[1] = category_dict.get(int(part[1]))
                        print(part[1])
                        part = ' '.join(part)
            
            translated_parts.append(part)
        
        # Соединяем обработанные части обратно в одну строку с "AND"
        translated_query = ' AND '.join(translated_parts)
        
        return translated_query
    
    translated_queries = [clean_query(query) for query in queries]
    return translated_queries


def query_dict(query_key) -> str:
    """Возвращает строку SQL-запроса в зависимости от ключа"""
    queries = {
        'main_query': '''
            SELECT a.first_name, a.last_name, f.title, f.description, f.release_year
            FROM film f
            {joins}
            {filters}
            {group_by}
            LIMIT 10
        ''',
        'actor_join': 'JOIN film_actor fa ON f.film_id = fa.film_id JOIN actor a ON fa.actor_id = a.actor_id',
        'category_join': 'JOIN film_category fc ON f.film_id = fc.film_id JOIN category c ON fc.category_id = c.category_id',
        'group_by_film': 'GROUP BY f.title',
        'group_by_actor': 'GROUP BY a.actor_id',
        'group_by_category': 'GROUP BY c.category_id',
        'rating_query': 'SELECT rating_list FROM rating',
        'category_query': 'SELECT category_id, name FROM category',
        'actor_query': 'SELECT first_name, last_name FROM actor ORDER BY last_name'
    }
    return queries.get(query_key)


def fetch_category_dict():
    """Получение словаря категорий из базы данных"""
    connection = ich_connect()
    cursor = connection.cursor()
    cursor.execute(query_dict('category_query'))
    categories = cursor.fetchall()
    cursor.close()
    disconnect(connection)
    return {category_id: name for category_id, name in categories}


def insert_query(query_key):
    """Вставка запроса в таблицу рейтинга"""
    connection = mydb_connect()
    cursor = connection.cursor()
    query = 'INSERT INTO rating (rating_list) VALUES (%s)'
    cursor.execute(query, (query_key,))
    connection.commit()
    cursor.close()
    disconnect(connection)


def fetch_category_list():
    """Получение списка категорий и вывод его в виде таблицы"""
    results = fetch_category_dict()
    
    # Преобразуем словарь категорий в список кортежей (ID, название)
    category_tuples = [(category_id, name) for category_id, name in results.items()]
    
    # Разбиваем список категорий на колонки
    column_count = 4
    columns = [[] for _ in range(column_count)]
    
    for index, (category_id, name) in enumerate(category_tuples):
        column_index = index % column_count
        columns[column_index].append((category_id, name))
    
    tables = []
    headers = ['№', 'Жанр']
    for col_index, col in enumerate(columns):
        table = []
        for row_index, (category_id, name) in enumerate(col, start=1):
            table.append(["{}".format(category_id), name])
        tables.append(tabulate(table, headers=headers, tablefmt='rounded_grid'))
    
    max_lines = max(len(table.split('\n')) for table in tables)
    
    for i in range(max_lines):
        for table in tables:
            table_lines = table.split('\n')
            if i < len(table_lines):
                print(table_lines[i].ljust(20), end=' ')
            else:
                print(' ' * 20, end=' ')
        print()
    
    return results


def create_table(table):
    """Создание и отображение меню выбора"""
    table_list = {
        'main_table': [['1', 'Название фильма'], ['2', 'Имя актёра'], ['3', 'Жанр\nГод выпуска'],
                       ['4', 'Показать 5 самых частых запросов'], ['0', 'Выход']],
        'category_year_table': [['1', 'Жанр'], ['2', 'Год выпуска'], ['3', 'Жанр и\nГод выпуска'], ['0', 'Выход']],
    }
    clear_screen()
    table = table_list.get(table)
    print(tabulate(table, tablefmt='rounded_grid'))


def show_top_queries():
    """Показ самых частых запросов с заменой ID категорий на их названия"""
    top_queries = get_top_queries()
    category_dict = fetch_category_dict()
    translated_queries = []
    
    for query, count in top_queries:
        # Разбиваем запрос по "AND", обрабатываем каждую часть отдельно
        parts = query.split(' AND ')
        translated_parts = []
        for part in parts:
            translated_part = translate_queries([part], category_dict)[0]
            translated_parts.append(translated_part)
        # Соединяем обработанные части обратно в одну строку с "AND"
        translated_query = ' и '.join(translated_parts)
        translated_queries.append([translated_query, count])
    
    clear_screen()
    print("Самые частые запросы:")
    print(tabulate(translated_queries, headers=["Запросы", "Количество"], tablefmt='rounded_grid'))
    input("\nНажмите Enter, чтобы вернуться в главное меню...")


def clear_screen():
    """Очистка экрана"""
    os.system('cls' if os.name == 'nt' else 'clear')


def get_top_queries() -> list:
    """Получение топ-5 самых частых запросов из таблицы рейтинга"""
    connection = mydb_connect()
    cursor = connection.cursor()
    query = 'SELECT rating_list, COUNT(*) as count FROM rating GROUP BY rating_list ORDER BY count DESC LIMIT 5'
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    disconnect(connection)
    return results


def filter_selection_title() -> str:
    """Фильтрация по названию фильма"""
    title = input('Введите название фильма: ').upper()
    return "f.title LIKE '%{}%'".format(title)


def filter_selection_actor() -> str:
    """Фильтрация по имени и фамилии актера"""
    filters = []
    actor_first_name = input('Введите имя актёра: ').upper()
    if actor_first_name:
        filters.append("a.first_name LIKE '%{}%'".format(actor_first_name))
    actor_last_name = input('Введите фамилию актёра: ').upper()
    if actor_last_name:
        filters.append("a.last_name LIKE '%{}%'".format(actor_last_name))
    return ' AND '.join(filters)


def filter_selection_category_year() -> str:
    """Фильтрация по категории и/или году выпуска"""
    filters = []
    create_table('category_year_table')
    user_choice = input('Выберите один из вариантов цифрами: ')
    if '1' in user_choice:
        fetch_category_list()
        category_id = input('Выберите один из жанров цифрами: ')
        filters.append("fc.category_id LIKE {}".format(category_id))
    if '2' in user_choice:
        year = input('Введите год выпуска: ')
        filters.append("f.release_year LIKE {}".format(year))
    if '3' in user_choice:
        fetch_category_list()
        category_id = input('Выберите один из жанров цифрами: ')
        filters.append("fc.category_id LIKE {}".format(category_id))
        year = input('Введите год выпуска: ')
        filters.append("f.release_year LIKE {}".format(year))
    return ' AND '.join(filters)


def filter_selection(user_choice) -> str:
    """Выбор фильтрации по выбранному пользователем критерию"""
    filters = []
    if '1' in user_choice:
        filters.append(filter_selection_title())
    if '2' in user_choice:
        filters.append(filter_selection_actor())
    if '3' in user_choice:
        filters.append(filter_selection_category_year())
    if '4' in user_choice:
        show_top_queries()
    if '0' in user_choice:
        quit()
    if filters:
        return 'WHERE ' + ' AND '.join(filters)
    return ''


def film_list(query_filter):
    """Получение и отображение списка фильмов по заданным фильтрам"""
    connection = ich_connect()
    cursor = connection.cursor()
    query = query_dict('main_query').format(
        joins=query_dict('actor_join') + ' ' + query_dict('category_join'),
        filters=query_filter,
        group_by=query_dict('group_by_film')
    )
    cursor.execute(query)
    results = cursor.fetchall()
    if not results:
        print('Нет данных по вашему запросу. Нажмите Enter, чтобы вернуться в главное меню...')
        input()
        return
    clear_screen()
    print(tabulate(results, headers=['Имя актера', 'Фамилия актера', 'Название фильма', 'Описание', 'Год выпуска'],
                   tablefmt='rounded_grid'))
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
        query_filter = filter_selection(choice)
        if query_filter:
            insert_query(query_filter)
            film_list(query_filter)