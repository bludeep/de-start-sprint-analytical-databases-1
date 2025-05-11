import logging
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models.variable import Variable

import boto3
import vertica_python

log = logging.getLogger(__name__)


@dag(
    dag_id='s3_load_s6_project',
    start_date=datetime(2022, 1, 1),
    schedule_interval='0/30 * * * *',  # Запуск каждые 30 минут
    catchup=False,
    description='Загрузка данных из S3 в Vertica',
    is_paused_upon_creation=True  # DAG создается в приостановленном состоянии
)
def project6_dag():

    file_name = 'group_log.csv'
    SCHEMA_NAME = 'STV202504020'

    @task(task_id="start")
    def start_task():
        """
        Задача запуска процесса обработки.
        Выводит имя файла, который будет обработан.
        """
        logging.info(f"Файл для обработки: {file_name}")

    @task(task_id="load_s3_file")
    def load_s3_file():
        """
        Загрузка файла из хранилища S3.
        Получает учетные данные из переменных Airflow и скачивает файл.
        """
        logging.info('Начало загрузки из S3')

        # Получение учетных данных из переменных Airflow
        AWS_ACCESS_KEY_ID = Variable.get('aws_access_key')
        AWS_SECRET_ACCESS_KEY = Variable.get('aws_secret_key')

        # Инициализация сессии S3
        session = boto3.session.Session()
        s3_client = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

        # Скачивание файла
        logging.info(f'Скачивание файла {file_name}')
        s3_client.download_file(
            Bucket='sprint6',
            Key=file_name,
            Filename=f'/data/{file_name}'
        )

        return True

    @task(task_id="prepare_data_for_loading")
    def prepare_data_for_loading():
        """
        Подготовка данных для загрузки в Vertica.
        Чтение CSV-файла, обработка данных и сохранение в новый файл.
        """
        logging.info('Подготовка данных для загрузки в Vertica')

        try:
            # Загрузка данных CSV в DataFrame pandas
            df_group_log = pd.read_csv(f'/data/{file_name}')

            # Преобразование столбца user_id_from в Int64 для корректной обработки NULL
            df_group_log['user_id_from'] = pd.array(
                df_group_log['user_id_from'], dtype="Int64")

            # Сохранение обработанного файла
            processed_file = '/data/processed_group_log.csv'
            df_group_log.to_csv(processed_file, index=False)
            logging.info(
                f"Успешно подготовлены данные для загрузки: {processed_file}")
        except Exception as e:
            logging.error(f"Ошибка обработки {file_name}: {e}")
            raise

        return True

    @task(task_id="load_to_staging")
    def load_to_staging():
        """
        Загрузка данных в промежуточную таблицу Vertica.
        Подключение к БД и копирование данных из CSV.
        """
        logging.info('Загрузка данных в промежуточную таблицу Vertica')

        # Параметры подключения к Vertica
        conn_info = {
            'host': Variable.get("VERTICA_HOST"),
            'port': '5433',
            'user': Variable.get("VERTICA_USER"),
            'password': Variable.get("VERTICA_PASSWORD"),
            'database': Variable.get("VERTICA_DB"),
            'autocommit': True
        }

        with vertica_python.connect(**conn_info) as connection:
            cursor = connection.cursor()

            try:
                processed_file = '/data/processed_group_log.csv'

                # Очистка и загрузка данных в Vertica
                cursor.execute(
                    f"TRUNCATE TABLE {SCHEMA_NAME}__STAGING.group_log CASCADE;")
                cursor.execute(f"""
                COPY {SCHEMA_NAME}__STAGING.group_log(group_id, user_id, user_id_from, event, event_datetime)
                    FROM LOCAL '{processed_file}'
                    DELIMITER ','
                    SKIP 1
                    NULL ''
                    REJECTED DATA AS TABLE {SCHEMA_NAME}__STAGING.group_log_rej
                    REJECTMAX 100;
                """)
                result = cursor.fetchall()
                logging.info(f"Загружены данные в таблицу group_log: {result}")
            except Exception as e:
                logging.error(f"Ошибка загрузки данных в Vertica: {e}")
                raise

        return True

    @task(task_id="transfer_to_vertica_dds_links_task_id")
    def transfer_to_vertica_dds_links_task():
        """
        Передача данных в таблицу связей DWH.
        Заполнение таблицы l_user_group_activity.
        """
        logging.info('Начало переноса данных в таблицу связей Vertica DWH')
        conn_info = {
            'host': Variable.get("VERTICA_HOST"),
            'port': '5433',
            'user': Variable.get("VERTICA_USER"),
            'password': Variable.get("VERTICA_PASSWORD"),
            'database': Variable.get("VERTICA_DB"),
            'autocommit': True
        }

        with vertica_python.connect(**conn_info) as connection:
            cursor = connection.cursor()
            try:
                # Запрос на вставку данных в таблицу связей
                cursor.execute(f"""
                INSERT INTO {SCHEMA_NAME}__DWH.l_user_group_activity(
                    hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
                SELECT DISTINCT
                    hash(hu.hk_user_id, hg.hk_group_id) as hk_l_user_group_activity,
                    hu.hk_user_id,
                    hg.hk_group_id,
                    NOW() as load_dt,
                    's3' as load_src
                FROM {SCHEMA_NAME}__STAGING.group_log as sgl
                LEFT JOIN {SCHEMA_NAME}__DWH.h_users hu ON hu.user_id = sgl.user_id
                LEFT JOIN {SCHEMA_NAME}__DWH.h_groups hg ON hg.group_id = sgl.group_id
                WHERE hu.hk_user_id IS NOT NULL 
                  AND hg.hk_group_id IS NOT NULL
                  AND hash(hu.hk_user_id, hg.hk_group_id) NOT IN (
                    SELECT hk_l_user_group_activity FROM {SCHEMA_NAME}__DWH.l_user_group_activity
                );
                """)
                result = cursor.fetchall()
                logging.info(
                    f"Строки, загруженные из STG group_log в DWH l_user_group_activity: {result}")
            except Exception as e:
                logging.error(
                    f"Ошибка переноса данных в таблицы связей DWH: {e}")
                raise

        return True

    @task(task_id="transfer_to_vertica_dds_satellites_task_id")
    def transfer_to_vertica_dds_satellites_task():
        """
        Передача данных в таблицы-сателлиты DWH.
        Заполнение таблицы s_auth_history.
        """
        logging.info('Начало переноса данных в таблицы-сателлиты Vertica DWH')
        conn_info = {
            'host': Variable.get("VERTICA_HOST"),
            'port': '5433',
            'user': Variable.get("VERTICA_USER"),
            'password': Variable.get("VERTICA_PASSWORD"),
            'database': Variable.get("VERTICA_DB"),
            'autocommit': True
        }

        with vertica_python.connect(**conn_info) as connection:
            cursor = connection.cursor()
            try:
                # Запрос на вставку данных в таблицу-сателлит
                cursor.execute(f"""
                INSERT INTO {SCHEMA_NAME}__DWH.s_auth_history(
                    hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
                SELECT
                    luga.hk_l_user_group_activity,
                    sgl.user_id_from,
                    sgl.event,
                    sgl.event_datetime AS event_dt,
                    NOW() AS load_dt,
                    's3' AS load_src
                FROM {SCHEMA_NAME}__STAGING.group_log AS sgl
                LEFT JOIN {SCHEMA_NAME}__DWH.h_users hu ON sgl.user_id = hu.user_id
                LEFT JOIN {SCHEMA_NAME}__DWH.h_groups hg ON sgl.group_id = hg.group_id
                LEFT JOIN {SCHEMA_NAME}__DWH.l_user_group_activity AS luga 
                    ON hg.hk_group_id = luga.hk_group_id AND hu.hk_user_id = luga.hk_user_id
                WHERE luga.hk_l_user_group_activity IS NOT NULL;
                """)
                result = cursor.fetchall()
                logging.info(
                    f"Строки, загруженные из STG group_log в DWH s_auth_history: {result}")
            except Exception as e:
                logging.error(
                    f"Ошибка переноса данных в таблицы-сателлиты DWH: {e}")
                raise

        return True

    @task(task_id="end_process")
    def end_process():
        """
        Завершение процесса обработки данных.
        Вывод сообщения об успешном завершении.
        """
        logging.info("Пайплайн обработки данных group_log успешно завершен")

    # Определение задач
    start = start_task()
    load_file = load_s3_file()
    prepare_data = prepare_data_for_loading()
    load_staging = load_to_staging()
    transfer_links = transfer_to_vertica_dds_links_task()
    transfer_satellites = transfer_to_vertica_dds_satellites_task()
    end = end_process()

    # Установка зависимостей между задачами
    start >> load_file >> prepare_data >> load_staging >> transfer_links >> transfer_satellites >> end


# Создание экземпляра DAG
group_log_dag = project6_dag()
