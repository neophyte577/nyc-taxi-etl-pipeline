# This script corresponds to the 'extract' step of the ETL pipeline process
# Paste this code into a newly created (generic) Mage data loader block

import io
import pandas as pd
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    
    file_url = 'https://storage.googleapis.com/taxi-data-engineering-project-1337/taxi_data.parquet'

    response = requests.get(file_url)
    response.raise_for_status()

    return pd.read_parquet(io.BytesIO(response.content))


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
