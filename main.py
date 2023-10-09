import datetime
import logging
from typing import Any
from inspect import getsourcefile
from os.path import abspath

import pandas as pd
from environs import Env

from yandex_direct_api import DirectApi
from db_connector import DBConnector



def get_db(env: Env) -> DBConnector:
    host = env.str("db_host")
    db_name = env.str("db_name")
    db_user = env.str("db_user")
    db_password = env.str("db_password")
    db = DBConnector(host, db_name, db_user, db_password)
    return db


def get_direct_api(env: Env) -> DBConnector:
    direct_login = env.str("direct_login")
    direct_access_token = env.str("direct_access_token")
    direct_api = DirectApi(direct_access_token, direct_login)
    return direct_api


def db_is_updated(db: DBConnector) -> bool:
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    last_day_in_db = db.get_last_date_in_db()
    return yesterday == last_day_in_db


def get_date_range_for_request_api(db: DBConnector) -> tuple[str, str]:
    one_day = datetime.timedelta(days=1)
    yesterday_included = datetime.date.today() - one_day
    start_from = db.get_last_date_in_db() + one_day 
    return (start_from, yesterday_included)


def data_preparation(raw_data: str) -> list[list[Any]]:
    data = [line.split("\t") for line in raw_data.strip().split("\n")]
    columns = data[0]
    del data[0]

    df = pd.DataFrame(data, columns=columns)

    df['Cost'] = df['Cost'].astype(float)
    df['Impressions'] = df['Impressions'].astype(int)
    df['Clicks'] = df['Clicks'].astype(int)
    df['CampaignId'] = df['CampaignId'].astype(int)
    df['Date'] = "'" + df['Date'].astype(str) + "'"

    list_of_lists = df.values.tolist()
    return list_of_lists


def get_path_to_dotenv() -> str:
    path_to_cur_file = abspath(getsourcefile(lambda:0))
    last_slash = path_to_cur_file.rfind("/")
    path_to_cur_dir = path_to_cur_file[:last_slash]
    path_to_env_file = path_to_cur_dir + "/.env"
    return path_to_env_file


def main():
    env = Env()
    path_to_env_file = get_path_to_dotenv()
    env.read_env(path_to_env_file)
    db = get_db(env)

    api = get_direct_api(env)

    if db_is_updated(db):
        logging.info("Database is updated")
        return

    date_range = get_date_range_for_request_api(db)
    raw_data_from_api = api.get_report_by_range(*date_range)
    preparated_data = data_preparation(raw_data_from_api)
    db.insert_many_rows(preparated_data)
    logging.info("Data uploaded")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
