import boto3
import os

def list_parquet_csv_files(bucket_name, prefix='', region_name='us-west-2', suffix=('.parquet', '.csv', '.txt', '.tar')):
    """
    Рекурсивно перечисляет все файлы указанных типов в S3-бакете и подпапках,
    начиная с заданного префикса.

    Аргумнеты:
        bucket_name (str): Имя S3-бакета
        prefix (str): Префикс для фильтрации файлов (например, 'merra2/')
        region_name (str): Регион AWS (по умолчанию 'us-west-2')
        suffix (tuple): Кортеж с указанием суффиксов искомых файлов

    Возвращает:
        list: Список кортежей (filename, full_path), где:
              filename - имя файла (без префикса)
              full_path - полный путь к файлу в S3 (s3://bucket/path)
    """

    s3 = boto3.client('s3', region_name=region_name, config=boto3.session.Config(signature_version=UNSIGNED)) # Без ключей доступа
    results = []

    def list_objects(current_prefix, suffix):
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix=current_prefix,
            Delimiter='/'  # Очень важно для обработки "папок"
        )

        # Обработка файлов в текущем префиксе
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith(suffix):
                    filename = os.path.basename(key)
                    full_path = f"s3://{bucket_name}/{key}"
                    results.append((filename, full_path))

        # Рекурсивный вызов для обработки подпапок
        if 'CommonPrefixes' in response:
            for prefix in response['CommonPrefixes']:
                list_objects(prefix['Prefix'])

    list_objects(prefix)
    return results

#--------------------------------------------------------------------------------------------------------
# Использование:
bucket_name = 'ai2-public-datasets'  # имя бакета
prefix = ''  # нужный префикс (или пустой для корня бакета)
region_name = 'us-west-2' # Замените регион, если необходимо

# Создаем "фиктивные" переменные окружения, чтобы boto3 не пытался искать ключи доступа
os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
os.environ['AWS_SECURITY_TOKEN'] = 'testing'
os.environ['AWS_SESSION_TOKEN'] = 'testing'

from botocore import UNSIGNED
from botocore.config import Config
# Config = Config(signature_version=UNSIGNED)

file_list = list_parquet_csv_files(bucket_name, prefix, region_name)

for filename, full_path in file_list:
    print(f"Filename: {filename}, Full Path: {full_path}")