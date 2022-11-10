# Отчётный проект
Обработка, анализ и визуализация данных обращений к файлам и наличию уязвимостей, получаемых с серверов Компании, с целью выявления потенциально опасных активностей


# Цели проекта
1. Реализовать парсинг и загрузку данных об обнаруженных файлах с потенциально опасными расширениями, а так же информации об обнаруженных уязвимостях
2. Обеспечить сохранение этих данных в СУБД для возможности дальнейшей обработки и анализа
3. Реализовать аналитику и визуализацию данных по заданным критериям для выявления потенциально опасных инцидентов

# Используемые технологии
1. Yandex Cloud
2. Apache Airflow
3. Python
4. PostgreSQL
5. Yandex DataLens


# Архитектура
![image](https://user-images.githubusercontent.com/67660495/201094914-725d2de1-3452-4eba-b649-7cfb7cdec20b.png)

# Оркестрация Apache Airflow
![image](https://user-images.githubusercontent.com/67660495/201095475-18c6ff6a-b38b-43aa-aa05-3ac078f9fcfb.png)

# Apache Airflow работа DAG
![image](https://user-images.githubusercontent.com/67660495/201095560-ac9d97c9-73e7-4f9a-a081-6503294aaa90.png)
![image](https://user-images.githubusercontent.com/67660495/201095589-a497abef-12c9-4731-985d-7fc0c46e8e64.png)

# Визуализация и дашборды
![image](https://user-images.githubusercontent.com/67660495/201095653-252ef8d7-09f3-463c-8a18-239085a76046.png)
![image](https://user-images.githubusercontent.com/67660495/201095670-7af6cb70-abf2-45e6-b43e-eeabdac7d252.png)
![image](https://user-images.githubusercontent.com/67660495/201095689-44f953b5-d0a3-4946-a589-adfba6adfc9b.png)
![image](https://user-images.githubusercontent.com/67660495/201095723-740f0e4d-6851-44f4-8a9f-0ef2b8be2ff9.png)

# Перечень файлов проекта
Подключение к СУБД

```bash
def insert_vulnerability_func():
    ps_pg_hook = PostgresHook(postgres_conn_id="con_ya_postgresql")
    conn_ps = ps_pg_hook.get_conn()
    cursor = conn_ps.cursor()
```   

###testcsv.py
https://github.com/IvanovaDarya/OTUS_project/blob/main/testcsv.py

DAG, ообеспечивающий парсинг данных из источника с файлами *csv  и импорт в хранилище данных

```bash
dag = DAG(
    dag_id='vulnerability',
    schedule_interval='*/30 * * * *',
    #0 18 * */3 *
    catchup = False,
    start_date=days_ago(1)
)
    
```
### testxml.py
https://github.com/IvanovaDarya/OTUS_project/blob/main/testxml.py

DAG, ообеспечивающий парсинг данных из источника с файлами  *xml и импорт в хранилище данных

```bash
   dag = DAG(
    dag_id='csv_parsing34',
    schedule_interval='*/30 * * * *',
    catchup = False,
    start_date=days_ago(1)
)
```
