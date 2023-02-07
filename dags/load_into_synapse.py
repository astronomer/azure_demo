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
    "azure_data_factory_conn_id": "adf_default",
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
    dag_id="load_fire_into_synapse",
    start_date=datetime(2021, 8, 13),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["async", "Azure Pipeline"],
) as dag:

    begin = EmptyOperator(task_id="start")

    wasbtosynapase = WasbToSynapseOperator(
        task_id='test_transfer',
        # Make sure to rename files
        source_name= "Fire_Department_Calls_for_Service.csv",
        destination_name="sf_fire_calls",
        resource_group_name=RESOURCE_GROUP,
        # Need to create one factory per file?
        factory_name="providersdf",
        # Activity ties to factory
        activity_name="viraj_copy_fire_data",
        # This is a constant
        translator_type=TRANSLATOR_TYPE,
        mappings=[
            {"source": {"name": "prop_0"}, "sink": {"name": "prop"}},
            {"source": {"name": "Call Number"}, "sink": {"name": "call_number"}},
            {"source": {"name": "Unit Id"}, "sink": {"name": "unit_id"}},
            {"source": {"name": "Incident Number"}, "sink": {"name": "incident_number"}},
            {"source": {"name": "Call Type"}, "sink": {"name": "call_type"}},
            {"source": {"name": "Call Date"}, "sink": {"name": "call_date"}},
            {"source": {"name": "Watch Date"}, "sink": {"name": "watch_date"}},
            {"source": {"name": "Received DtTm"}, "sink": {"name": "received_time"}},
            {"source": {"name": "EntryDtTm"}, "sink": {"name": "entry_time"}},
            {"source": {"name": "DispatchDtTm"}, "sink": {"name": "dispatch_time"}},
            {"source": {"name": "ResponseDtDm"}, "sink": {"name": "response_time"}},
            {"source": {"name": "On Scene DtTm"}, "sink": {"name": "scene_time"}},
            {"source": {"name": "Transport DtTm"}, "sink": {"name": "transport_time"}},
            {"source": {"name": "Hospital DtTm"}, "sink": {"name": "hospital_time"}},
            {"source": {"name": "Call Final Disposition"}, "sink": {"name": "call_final_disposition"}},
            {"source": {"name": "Available DtTm"}, "sink": {"name": "avail_time"}},
            {"source": {"name": "Address"}, "sink": {"name": "Address"}},
            {"source": {"name": "City"}, "sink": {"name": "city"}},
            {"source": {"name": "Zipcode of Incident"}, "sink": {"name": "zipcode"}},
            {"source": {"name": "Battalion"}, "sink": {"name": "battalion"}},
            {"source": {"name": "Station Area"}, "sink": {"name": "station"}},
            {"source": {"name": "Box"}, "sink": {"name": "box"}},
            {"source": {"name": "Original Priority"}, "sink": {"name": "orig_priority"}},
            {"source": {"name": "Priority"}, "sink": {"name": "priority"}},
            {"source": {"name": "Final Priority"}, "sink": {"name": "final_priority"}},
            {"source": {"name": "ALS Unit"}, "sink": {"name": "als_unit"}},
            {"source": {"name": "Call Type Group"}, "sink": {"name": "call_type_alarm"}},
            {"source": {"name": "Number of Alarms"}, "sink": {"name": "num_alarms"}},
            {"source": {"name": "Unit Type"}, "sink": {"name": "unit_type"}},
            {"source": {"name": "Unit sequence in call dispatch"}, "sink": {"name": "unit_squence"}},
            {"source": {"name": "Fire Prevention District"}, "sink": {"name": "fire_district"}},
            {"source": {"name": "Supervisor District"}, "sink": {"name": "s_district"}},
            {"source": {"name": "Neighborhoods - Analysis Boundaries"}, "sink": {"name": "neighborhooods"}},
            {"source": {"name": "RowID"}, "sink": {"name": "row_id"}},
            {"source": {"name": "case_location"}, "sink": {"name": "case_location"}},
            {"source": {"name": "Analysis Neighborhoods"}, "sink": {"name": "analysis_neighborhoods"}},

        ],
    )
        # Once all the data is loaded, run some datafactory job


    begin >> wasbtosynapase
