if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
import pandas as pd
import math

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
    if isinstance(data, bool):
        return False
    year = kwargs.get('year')
    month = kwargs.get('month')
    table_name = f'ny_taxi_trips_{year}_{month}'
    new_schema_name = kwargs.get('new_schema_name')
    config_path = kwargs.get('config_path')
    config_profile = kwargs.get('config_profile')
    
    size = 100000
    start = 0
    end = size
    num_chunks = math.ceil(data.shape[0]/size)

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            data.iloc[start:end],
            new_schema_name,
            table_name,
            index=False,
            if_exists='replace'
        )
        start = end
        end = min(end + size, data.shape[0])
        for i in range(1, num_chunks):
            loader.export(
                data.iloc[start:end],
                new_schema_name,
                table_name,
                index=False,
                if_exists='append'
            )
            start = end
            end = min(end + size, data.shape[0])


