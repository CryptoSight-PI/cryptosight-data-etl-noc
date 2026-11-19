import boto3
import os
from dotenv import load_dotenv, find_dotenv
import mysql.connector

load_dotenv(find_dotenv())

BUCKET_RAW = os.getenv("BUCKET_RAW")
BUCKET_TRUSTED = os.getenv("BUCKET_TRUSTED")
BUCKET_CURATED = os.getenv("BUCKET_CURATED")

session = boto3.Session()
s3 = session.client("s3")

conn = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "root"),
    "database": os.getenv("DB_NAME", "cryptosight"),
}

conexao = mysql.connector.connect(**conn)
cursor = conexao.cursor()