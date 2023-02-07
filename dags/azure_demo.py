import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from astronomer.providers.microsoft.azure.operators.synapse import WasbToSynapseOperator
from astronomer.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperatorAsync,
)

EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
RESOURCE_GROUP = "team_provider_resource_group_test"
TRANSLATOR_TYPE = "TabularTranslator"

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "azure_data_factory_conn_id": "azure_data_factory_default",
}


# fill out these params accordingly - need all the files here.
sf_data = [
    {"file": "test",
    "destination": "me",
    "load_factory": "file_factory",
    "activity": "active"},

    {"file": "test2",
    "destination": "me",
    "load_factory": "file_factory",
    "activity": "active"}
    ]


with DAG(
    dag_id="sf_real_estate_modeling",
    start_date=datetime(2021, 8, 13),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["async", "Azure Pipeline"],
) as dag:

    begin = EmptyOperator(task_id="start")

    data_modeling = AzureDataFactoryRunPipelineOperatorAsync(
        task_id='run_data',
        pipeline_name='test',
        resource_group_name=RESOURCE_GROUP,
        factory_name="model"
    )

    for sf in sf_data:
        wasbtosynapase = WasbToSynapseOperator(
            task_id=sf['file'],
            # Make sure to rename files
            source_name= "{source}.csv".format(source=sf["file"]),
            destination_name="{destination} ".format(destination=sf["file"]),
            resource_group_name=RESOURCE_GROUP,
            # Need to create one factory per file?
            factory_name="factory.{name}".format(name=sf["file"]),
            # Activity ties to factory
            activity_name="copy_activity_rajath",
            # This is a constant
            translator_type=TRANSLATOR_TYPE,
            mappings=[
                {"source": {"name": "column1"}, "sink": {"name": "col1"}},
                {"source": {"name": "column2"}, "sink": {"name": "col2"}},
            ],
        )
        # Once all the data is loaded, run some datafactory job


        begin >> wasbtosynapase >> data_modeling
