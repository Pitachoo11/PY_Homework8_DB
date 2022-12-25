import sqlite3 as sl
import pandas as pd
import numpy as np

def search_name(input):
    
    con = sl.connect('organization.db')
    sql_query = pd.read_sql_query('''
                                SELECT stuff.person_id, last_name, first_name, patronymic_name, dep_name, pos_name, tel_number, email 
                                FROM stuff
                                INNER JOIN telephones, email, departments, positions
                                ON stuff.person_id = telephones.person_id
                                AND stuff.person_id = email.person_id
                                AND stuff.person_id = departments.person_id
                                AND stuff.position_id = positions.position_id
                                AND stuff.last_name = ?
                                '''
                                ,con, params=(input,))
    
    print(f"{pd.DataFrame(sql_query)}")   

def export_txt():

    con = sl.connect('organization.db')

    sql_query = pd.read_sql_query('''
                                SELECT stuff.person_id, last_name, first_name, patronymic_name, dep_name, pos_name, tel_number, email, city, street, building, build_add1, build_add2, apartment  
                                FROM stuff
                                INNER JOIN telephones, addresses, email, departments, positions
                                ON stuff.person_id = telephones.person_id
                                AND stuff.person_id = addresses.person_id
                                AND stuff.person_id = email.person_id
                                AND stuff.person_id = departments.person_id
                                AND stuff.position_id = positions.position_id
                                '''
                                ,con) # here, the 'conn' is the variable that contains your database connection information from step 2

    df = pd.DataFrame(sql_query)

    np.savetxt(r'exported_DB.txt', df.values, fmt='%s')
    print('Экспорт файла txt завершен')

def export_csv():

    con = sl.connect('organization.db')

    sql_query = pd.read_sql_query('''
                                SELECT stuff.person_id, last_name, first_name, patronymic_name, dep_name, pos_name, tel_number, email, city, street, building, build_add1, build_add2, apartment  
                                FROM stuff
                                INNER JOIN telephones, addresses, email, departments, positions
                                ON stuff.person_id = telephones.person_id
                                 AND stuff.person_id = addresses.person_id
                                 AND stuff.person_id = email.person_id
                                 AND stuff.person_id = departments.person_id
                                AND stuff.position_id = positions.position_id
                                '''
                                ,con) # here, the 'conn' is the variable that contains your database connection information from step 2

    df = pd.DataFrame(sql_query)

    df.to_csv (r'exported_DB.csv', index = False)
    print('Экспорт файла csv завершен')

def input_DB():
    con = sl.connect('organization.db')

    # --- создаём таблицу с сотрудниками ---
    # открываем базу
    with con:
        # получаем количество таблиц с нужным нам именем
        data = con.execute("select count(*) from sqlite_master where type='table' and name='stuff'")
        for row in data:
            # если таких таблиц нет
            if row[0] == 0:
            
                # создаём таблицу для сотрудников
                with con:
                    con.execute("""
                        CREATE TABLE stuff (
                            person_id INTEGER PRIMARY KEY,
                            last_name VARCHAR(10),
                            first_name VARCHAR(10),
                            patronymic_name VARCHAR(20),
                            date_birth DATE,
                            position_id INTEGER,
                            department_id INTEGER,
                            FOREIGN KEY (position_id) REFERENCES positions(position_id),
                            FOREIGN KEY (department_id) REFERENCES departments(person_id)
                        );
                    """)

    # подготавливаем множественный запрос
    sql = 'INSERT INTO stuff (person_id, last_name, first_name, patronymic_name, date_birth, position_id, department_id) values(?, ?, ?, ?, ?, ?, ?)'
    # указываем данные для запроса
    data = [
        (1,'Иванов','Иван','Петрович','2010-03-12',1,1),
        (2,'Семенова','Ирина','Алексеевна','2009-05-25',2,2),
        (3,'Васечкин','Алексей','Иванович','2015-01-30',3,3),
        (4,'Петров','Николай','Алексеевич','2010-09-15',4,4)
    ]

    # добавляем с помощью множественного запроса все данные сразу
    with con:
        con.executemany(sql, data)

    # --- создаём таблицу с адресами ---
    # открываем базу
    with con:
        # получаем количество таблиц с нужным нам именем — adresses
        data = con.execute("select count(*) from sqlite_master where type='table' and name='addresses'")
        for row in data:
            # если таких таблиц нет
            if row[0] == 0:

                # создаём таблицу для клиентов
                with con:
                    con.execute("""
                        CREATE TABLE addresses (
                            person_id INTEGER,
                            city VARCHAR(20),
                            street VARCHAR(20),
                            building INTEGER,
                            build_add1 INTEGER,
                            build_add2 INTEGER,
                            apartment INTEGER,
                            FOREIGN KEY (person_id) REFERENCES stuff(person_id)
                        );
                    """)
    # подготавливаем множественный запрос
    sql = 'INSERT INTO addresses (person_id, city, street, building, build_add1, build_add2, apartment) values(?, ?, ?, ?, ?, ?, ?)'
    # указываем данные для запроса
    data = [
        (1,'Москва','Ломоносовская',19,1,2,3),
        (2,'Москва','Западная',78,2,3,4),
        (3,'Москва','Ленина',5,5,6,7),
        (4,'Краснодар','Просвещения',10,6,7,8)
    ]

    # добавляем с помощью множественного запроса все данные сразу
    with con:
        con.executemany(sql, data)

    # --- создаём таблицу с e-mail ---
    # открываем базу
    with con:
        # получаем количество таблиц с нужным нам именем — email
        data = con.execute("select count(*) from sqlite_master where type='table' and name='email'")
        for row in data:
            # если таких таблиц нет
            if row[0] == 0:

                # создаём таблицу для клиентов
                with con:
                    con.execute("""
                        CREATE TABLE email (
                            person_id INTEGER,
                            email VARCHAR(20),
                            FOREIGN KEY (person_id) REFERENCES stuff(person_id)
                        );
                    """)
    # подготавливаем множественный запрос
    sql = 'INSERT INTO email (person_id, email) values(?, ?)'
    # указываем данные для запроса
    data = [
        (1,'ivanov@org.ru'),
        (2,'Semenova@org.ru'),
        (3,'Vasechkin@org.ru'),
        (4,'Petrov@org.ru')
    ]

    # добавляем с помощью множественного запроса все данные сразу
    with con:
        con.executemany(sql, data)

    # --- создаём таблицу с телефонами ---
    # открываем базу
    with con:
        # получаем количество таблиц с нужным нам именем — telephones
        data = con.execute("select count(*) from sqlite_master where type='table' and name='telephones'")
        for row in data:
            # если таких таблиц нет
            if row[0] == 0:

                # создаём таблицу для клиентов
                with con:
                    con.execute("""
                        CREATE TABLE telephones (
                            person_id INTEGER,
                            tel_number VARCHAR(20),
                            FOREIGN KEY (person_id) REFERENCES stuff(person_id)
                        );
                    """)
    # подготавливаем множественный запрос
    sql = 'INSERT INTO telephones (person_id, tel_number) values(?, ?)'
    # указываем данные для запроса
    data = [
        (1,'89261112233'),
        (2,'89092223344'),
        (3,'89113334455'),
        (4,'89014445566')
    ]

    # добавляем с помощью множественного запроса все данные сразу
    with con:
        con.executemany(sql, data)

    # --- создаём таблицу с отделами ---
    # открываем базу
    with con:
        # получаем количество таблиц с нужным нам именем — departments
        data = con.execute("select count(*) from sqlite_master where type='table' and name='departments'")
        for row in data:
        # если таких таблиц нет
            if row[0] == 0:

                # создаём таблицу для клиентов
                with con:
                    con.execute("""
                        CREATE TABLE departments (
                            department_id INTEGER,
                            person_id INTEGER,
                            dep_name VARCHAR(20),
                            FOREIGN KEY (person_id) REFERENCES stuff(person_id)
                        );
                    """)
    # подготавливаем множественный запрос
    sql = 'INSERT INTO departments (department_id, person_id, dep_name) values(?, ?, ?)'
    # указываем данные для запроса
    data = [
        (1,1,'Производство'),
        (2,2,'Бухгалтерия'),
        (3,3,'Продажи'),
        (4,4,'ИТ')
    ]
    with con:
        con.executemany(sql, data)

    sql = 'INSERT INTO departments (department_id, dep_name) values(?, ?)'
    # указываем данные для запроса
    data = [
        (5,'СБ')
    ]

    # добавляем с помощью множественного запроса все данные сразу
    with con:
        con.executemany(sql, data)

    # --- создаём таблицу с должностями ---
    # открываем базу
    with con:
        # получаем количество таблиц с нужным нам именем — positions
        data = con.execute("select count(*) from sqlite_master where type='table' and name='positions'")
        for row in data:
            # если таких таблиц нет
            if row[0] == 0:

                # создаём таблицу для клиентов
                with con:
                    con.execute("""
                        CREATE TABLE positions (
                            position_id INTEGER,
                            pos_name VARCHAR(20),
                            salary INTEGER
                        );
                    """)
    # подготавливаем множественный запрос
    sql = 'INSERT INTO positions (position_id, pos_name, salary) values(?, ?, ?)'
    # указываем данные для запроса
    data = [
        (1,'Директор',100000),
        (2,'Бухгалтер',80000),
        (3,'Менеджер',70000),
        (4,'Специалист',60000)
    ]
    with con:
        con.executemany(sql, data)
    
    sql = 'INSERT INTO positions (position_id, pos_name, salary) values(?, ?, ?)'
    # указываем данные для запроса
    data = [
        (5,'Инженер',50000)
    ]

    # добавляем с помощью множественного запроса все данные сразу
    with con:
        con.executemany(sql, data)

    print('Загрузка начальных данных в БД завершена')