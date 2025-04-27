# pr_06
# Практическая работа 6. Аналитика с использованием сложных типов данных. Поиск и анализ продаж
## Цель
Рассмотреть данные переписи населения США об использовании общественного транспорта относительно ZIP-кода.
Требуется проанализировать, имеется ли корреляция между уровнем использования общественного транспорта с продажами в
организации в определенном регионе.
## Ход работы
1. Загрузите набор данных public_transportation_statistics_by_zip_code.csv с GitHub:
https://github.com/BosenkoTM/SQL-for-Begginer-DataAnalytics/blob/main/Datasets/public_transportation_statistics_by_zip_code.csv
2. Скопируйте данные public_transportation_statistics_by_zip_code.csv в базу данных клиентов организации, создав для них таблицу в наборе данных.
3. Найдите максимальное и минимальное процентное соотношение в данных. Значения ниже 0 принять как пустое значение.
4. Рассчитайте средний объем продаж для клиентов, проживающих в регионах с высоким уровнем использования общественного транспорта (более 10%), а также с низким уровнем использования общественного транспорта (менее или равным 10%).
5. Загрузить данные в pandas и построить гистограмму распределения (использовать my_data.plot.hist(y='public_transportation_pct') для построения гистограммы).
6. Сгруппируйте клиентов по их zip_code, и посмотрите на среднее количество транзакций на одного клиента. Экспортируйте эти данные в Excel и создайте диаграмму рассеяния, чтобы лучше понять взаимосвязь между использованием общественного транспорта и продажами.
7. На основании этого анализа, какие рекомендации вы бы дали руководству компании при рассмотрении возможностей расширения?

## Выполнение работы
Сначала подключимся к библиотеке psycopg2 и установим соединение с PostgreSQL
````
%pip install psycopg2
````
````
import psycopg2
from psycopg2 import Error
````
## 1. Подключаемся к заранее созданной базе данных и создаем там нужную нам таблицу public_transportation_statistics_by_zip_code  с которой будем в дальнейшем работать
````
import psycopg2

def get_connection(database_name):
    # Функция для получения подключения к базе данных
    connection = psycopg2.connect(user="postgres",
                                  password="26102006",
                                  host="localhost",
                                  port="5432",
                                  database=database_name)
    return connection

def close_connection(connection):
    # Функция для закрытия подключения к базе данных
    if connection:
        connection.close()
        print("Соединение с PostgreSQL закрыто")

try:
    connection = get_connection("BD1")
    cursor = connection.cursor()

    # Создание таблицы public_transportation_statistics_by_zip_code
    create_table_query = '''
    CREATE TABLE public_transportation_statistics_by_zip_code(
        zip_code character varying(10) NOT NULL PRIMARY KEY,
        public_transportation_pct numeric(15,2) NOT NULL,
        public_transportation_population integer
    );
    '''
    cursor.execute(create_table_query)
    connection.commit()
    print("Таблица 'public_transportation_statistics_by_zip_code' успешно создана")

except (Exception, psycopg2.Error) as error:
    print("Ошибка при подключении или работе с PostgreSQL:", error)

finally:
    # Закрытие подключения к базе данных
    if connection:
        close_connection(connection)
````
Получаем результат:

![image](https://github.com/user-attachments/assets/036dc77b-1d9b-4da1-a689-280fd210fce0)

## 2. Скопируем данные public_transportation_statistics_by_zip_code.csv в базу данных клиентов организации, в предворительно созданную нами таблицу 
````
try:
    connection = get_connection("BD1")
    cursor = connection.cursor()

# Путь к CSV-файлу
    csv_file_path = r"C:\Users\Пользователь\Desktop\SQL\public_transportation_statistics_by_zip_code.csv"

    # Проверка существования файла
    if not os.path.exists(csv_file_path):
        print(f"ОШИБКА: Файл '{csv_file_path}' не найден.")
        raise False # Возвращаем False при неудаче
    
    # Вставка данных из CSV-файла с использованием COPY
    with open(csv_file_path, 'r') as file:
        copy_query = """
        COPY public_transportation_statistics_by_zip_code(zip_code, public_transportation_pct, public_transportation_population)
        FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',');
        """
        cursor.copy_expert(copy_query, file)
        connection.commit()
        print("Данные успешно вставлены в таблицу 'public_transportation_statistics_by_zip_code'")

except (Exception, psycopg2.Error) as error:
    print("Ошибка при подключении или работе с PostgreSQL:", error)
````

Получаем результат:

![image](https://github.com/user-attachments/assets/98927422-149a-4012-84c2-ddfcd9ce2d67)

## Теперь найдем максимальное и минимальное процентное соотношение в данных. Значения ниже 0 будем принимать как пустое значение.
````
try:
    connection = get_connection("BD1")
    cursor = connection.cursor()

    # Запрос для поиска максимального и минимального значения процентного соотношения
    query = """
    SELECT 
        MAX(public_transportation_pct) AS max_pct,
        MIN(public_transportation_pct) AS min_pct
    FROM public_transportation_statistics_by_zip_code
    WHERE public_transportation_pct >= 0;
    """
    cursor.execute(query)
    result = cursor.fetchone()
    max_pct, min_pct = result
    print(f"Максимальное процентное соотношение: {max_pct}")
    print(f"Минимальное процентное соотношение: {min_pct}")
````
Получаем результат:
![image](https://github.com/user-attachments/assets/968de915-bb41-4912-9e29-f987a6bfa4ce)

## Теперь рассчитаем средний объем продаж для клиентов, проживающих в регионах с высоким уровнем использования общественного транспорта (более 10%), а также с низким уровнем использования общественного транспорта (менее или равным 10%).
````
try:
    connection = get_connection("BD1")
    cursor = connection.cursor()

    # SQL-запрос для расчета среднего объема продаж
    query = """
    SELECT 
    CASE 
        WHEN pts.public_transportation_pct > 10 THEN 'high_transport'
        ELSE 'low_transport'
    END AS "transport_level",
    AVG(s.sales_amount) AS "Average sales volume"
FROM 
    sales s
JOIN 
    customers c ON s.customer_id = c.customer_id
JOIN 
    public_transportation_statistics_by_zip_code pts ON c.postal_code = pts.zip_code
GROUP BY 
    CASE 
        WHEN pts.public_transportation_pct > 10 THEN 'high_transport'
        ELSE 'low_transport'
    END;
    """
    cursor.execute(query)
    results = cursor.fetchall()

# Вывод результатов
    print("Уровень использования транспорта | Средний объем продаж")
    for row in results:
        category, avg_sales = row
        print(f"{category} | {avg_sales}")
````
Получаем результат:
