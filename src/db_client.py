import mysql.connector
import logging
from config import Config

logger = logging.getLogger(__name__)

class MySQLClient:
    def __init__(self):
        self.connection = None

    def connect(self):
        self.connection = mysql.connector.connect(
            host=Config.DB_HOST,
            port=Config.DB_PORT,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            database=Config.DB_NAME,
        )
        logger.info("Connected to MySQL")

    def fetch_changes(self, last_updated, last_id):
        cursor = self.connection.cursor(dictionary=True)

        if last_updated:
            query = f"""
            SELECT *
            FROM {Config.TABLE_NAME}
            WHERE last_updated > %s
            OR (last_updated = %s AND id > %s)
            ORDER BY last_updated, id
            """
            cursor.execute(query, (last_updated, last_updated, last_id))
        else:
            query = f"""
            SELECT *
            FROM {Config.TABLE_NAME}
            ORDER BY last_updated, id
            """
            cursor.execute(query)

        results = cursor.fetchall()
        cursor.close()
        return results

    def close(self):
        if self.connection:
            self.connection.close()
            logger.info("MySQL connection closed")