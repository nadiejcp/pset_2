if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_ai.orchestration.triggers.api import trigger_pipeline
from mage_ai.settings.repo import get_repo_path
from os import path


@custom
def transform_custom(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    year = kwargs.get('year', 2014)
    month = kwargs.get('month', 12)
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    if int(year) <= 2020:
        next_month = int(month) + 1
        next_year = int(year)

        if next_month > 12:
            next_month = 1
            next_year += 1

        trigger_pipeline(
            'ny_taxi_dataset',
            variables={
                'year': str(next_year),
                'month': f'{next_month:02}',
                'schema': 'raw',
                'config_path': config_path,
                'config_profile': config_profile
            }
        )
        return {
            "current": f"{year}-{month:02}",
            "next": f"{next_year}-{next_month:02}",
            "status": "Working..."
        }

    return { "status": "Completed" }


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    print(output)
    assert output is not None, 'The output is undefined'
