if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres

from pandas import DataFrame
from os import path
import math
from tqdm.auto import tqdm

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    if not data:
        return False
    year = kwargs.get('year')
    month = kwargs.get('month')
    table_name = f'ny_taxi_trips_{year}_{month}'

    schema_name = 'raw'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    size = 100000

    start = 0
    end = size
    num_chunks = math.ceil(len(data)/size)
    
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            data.iloc[start:end],
            schema_name,
            table_name,
            index=False,
            if_exists='replace'
        )
        start = end
        end = min(end + size, len(data))
        for i in tqdm(range(1, num_chunks), desc="Cargando datos", unit="chunk"):
            loader.export(
                data.iloc[start:end],
                schema_name,
                table_name,
                index=False,
                if_exists='append'
            )
            start = end
            end = min(end + size, len(data))