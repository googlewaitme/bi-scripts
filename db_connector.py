import psycopg2
from psycopg2 import OperationalError
import logging
import datetime
from typing import List, Any


class DBConnector:
    def __init__(self, host, database, user, password):
        self.all_columns = ["Date", "Cost", "Impressions", "Clicks", "CampaignId"]
        self.str_all_columns = '"' + '", "'.join(self.all_columns) + '"'
        try:
            self.conn = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password
            )
            logging.info("database is connected")
        except OperationalError as e:
            logging.error(str(e))
        self.cursor = self.conn.cursor()

    def __del__(self):
        if self.conn:
            self.conn.close()
            logging.info("database is closed")

    def get_last_date_in_db(self) -> datetime.date:
        self.cursor.execute("""SELECT "Date" FROM public.direct_bi ORDER BY "Date" DESC LIMIT 1;""")
        last_date = self.cursor.fetchone()[0]
        return last_date

    def insert_row(self, values: List[Any], need_commit: bool = True) -> None:
        str_values = ', '.join(list(map(str, values)))
        insert_request = f"""
            INSERT INTO public.direct_bi({self.str_all_columns}) 
            VALUES ({str_values})
        """
        logging.debug(insert_request)
        self.cursor.execute(insert_request)
        if need_commit:
            self.conn.commit()

    def insert_many_rows(self, rows: List[List[Any]]) -> None:
        for row in rows:
            self.insert_row(row, need_commit=False)
        self.conn.commit()