import pandas as pd
from config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
from sqlalchemy import create_engine


conn_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conn_str)


def stage_data(data, db_name:str):
    pd.DataFrame(data).to_sql(db_name, con=engine, if_exists="append", index=False)