if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
import pandas as pd

@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    year = kwargs.get('year')
    month = kwargs.get('month')
    schema = kwargs.get('schema')
    config_path = kwargs.get('config_path')
    config_profile = kwargs.get('config_profile')
    if not year or not month or not schema or not config_path or not config_profile:
        raise ValueError("Missing year or month or schema")
        
    table_name = f'ny_taxi_trips_{year}_{month}'
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.open()
        conn = loader.conn
        cur = conn.cursor()

        cur.execute(f"""
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables 
                        WHERE table_schema = '{schema}'
                        AND table_name = '{table_name}'
                    );
                """)

        exists = pd.read_sql(exists_query, conn)
        if exists.iloc[0, 0]:
            print("Table already populated, skipping")
            return False
    
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_year-month.parquet"  
    url = base_url.replace('year', year).replace('month', month)
    return pd.read_parquet(url)


@test
def test_output(*args) -> None:
    """
    Template code for testing the output of the block.
    """
    pass
