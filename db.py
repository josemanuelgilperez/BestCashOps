import os
import mysql.connector
from mysql.connector import pooling
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
}


def get_connection():
    """Devuelve una conexión simple a la BD."""
    return mysql.connector.connect(**DB_CONFIG)


_pool = None


def get_pool(pool_name: str = "bestcash_pool", pool_size: int = 5):
    """Devuelve (y crea si hace falta) un pool de conexiones MySQL."""
    global _pool
    if _pool is None:
        _pool = pooling.MySQLConnectionPool(
            pool_name=pool_name,
            pool_size=pool_size,
            **DB_CONFIG,
        )
    return _pool

