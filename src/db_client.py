import time
import mysql.connector
import logging
from config import Config

logger = logging.getLogger(__name__)

class MySQLClient:
    def __init__(self):
        self.connection = None

    def connect(self, max_retries=10, delay=3):
        retries = 0

        while retries < max_retries:
            try:
                self.connection = mysql.connector.connect(
                    host=Config.DB_HOST,
                    port=Config.DB_PORT,
                    user=Config.DB_USER,
                    password=Config.DB_PASSWORD,
                    database=Config.DB_NAME,
                )
                logger.info("Connected to MySQL")
                return

            except mysql.connector.Error as e:
                retries += 1
                logger.warning(
                    f"MySQL connection failed (attempt {retries}/{max_retries}): {e}"
                )
                time.sleep(delay)

        raise Exception("Failed to connect to MySQL after retries")

    def fetch_changes(self, last_updated):
        cursor = self.connection.cursor(dictionary=True)

        if last_updated:
            query = f"""
                SELECT *
                FROM {Config.TABLE_NAME}
                WHERE last_updated > %s
                ORDER BY last_updated ASC, id ASC
            """
            cursor.execute(query, (last_updated,))
        else:
            query = f"""
                SELECT *
                FROM {Config.TABLE_NAME}
                ORDER BY last_updated ASC, id ASC
            """
            cursor.execute(query)

        results = cursor.fetchall()
        cursor.close()
        return results

    def close(self):
        if self.connection:
            self.connection.close()
            logger.info("MySQL connection closed")
