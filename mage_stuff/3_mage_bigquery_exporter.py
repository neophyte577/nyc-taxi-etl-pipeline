# This script corresponds to the 'load' step of the ETL pipeline process
# Paste this code into a newly created Google BigQuery data exporter block in Mage 

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_big_query(data, **kwargs) -> None:
    """
    Template for exporting data to a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """

    for key in data:

        table_id = 'taxi-data-444600.taxi_data_engineering.' + key 
        config_path = path.join(get_repo_path(), 'io_config.yaml')
        config_profile = 'default'

        BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
            data[key],
            table_id,
            if_exists='replace', 
        )
