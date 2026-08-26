import mysql.connector

from .config import ShopifySyncConfig


def get_connection(config: ShopifySyncConfig):
    config.validate_for_db()
    return mysql.connector.connect(
        host=config.db_host,
        port=config.db_port,
        user=config.db_user,
        password=config.db_password,
        database=config.db_name,
        autocommit=True,
    )
