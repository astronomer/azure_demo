import os
from datetime import datetime, timedelta

from airflow import DAG

from astronomer.providers.microsoft.azure.operators.synapse import WasbToSynapseOperator
from astronomer.providers.microsoft.azure.operators.synapse_sql import (
    SynapseSQLOperator,
)
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
RESOURCE_GROUP = "team_provider_resource_group_test"
TRANSLATOR_TYPE = "TabularTranslator"

EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "azure_data_factory_conn_id": "adf_default",
}

with DAG(
    dag_id="example_synapse",
    start_date=datetime(2021, 8, 13),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "Synapse", "Azure"],
) as dag:
    wasb_to_synapse = WasbToSynapseOperator(
        task_id="wasb_to_synapse",
        source_name="DelimitedText3",
        destination_name="AzureSynapseAnalyticsTable1",
        resource_group_name="team_provider_resource_group_test",
        factory_name="providersdf",
        activity_name="viraj_sf_business_locations",
        translator_type="TabularTranslator",
        mappings=[
            {"source": {"name": "Location Id"}, "sink": {"name": "location_id"}},
            {"source": {"name": "Business Account Number"}, "sink": {"name": "acct_number"}},
            {"source": {"name": "Ownership Name"}, "sink": {"name": "owner_name"}},
            {"source": {"name": "DBA Name"}, "sink": {"name": "dba_name"}},
            {"source": {"name": "Street Address"}, "sink": {"name": "address"}},
            {"source": {"name": "City"}, "sink": {"name": "city"}},
            {"source": {"name": "State"}, "sink": {"name": "state"}},
            {"source": {"name": "Source Zipcode"}, "sink": {"name": "zipcode"}},
            {"source": {"name": "Business Start Date"}, "sink": {"name": "start_date"}},
            {"source": {"name": "Business End Date"}, "sink": {"name": "end_date"}},
            {"source": {"name": "Location Start Date"}, "sink": {"name": "loc_start_date"}},
            {"source": {"name": "Location End Date"}, "sink": {"name": "loc_end_date"}},
            {"source": {"name": "Mail Address"}, "sink": {"name": "mail_address"}},
        ],
    )

    # wasbtosynapase_fire = WasbToSynapseOperator(
    #     task_id='fire_data',
    #     # Make sure to rename files
    #     source_name= "fire_data",
    #     destination_name="AzureSynapseAnalyticsTable2",
    #     resource_group_name=RESOURCE_GROUP,
    #     # Need to create one factory per file?
    #     factory_name="providersdf",
    #     # Activity ties to factory
    #     activity_name="viraj_copy_fire_data",
    #     # This is a constant
    #     translator_type=TRANSLATOR_TYPE,
    #     mappings=[
    #         {"source": {"name": "Call Number"}, "sink": {"name": "call_number"}},
    #         {"source": {"name": "Unit ID"}, "sink": {"name": "unit_id"}},
    #         {"source": {"name": "Incident Number"}, "sink": {"name": "incidient_number"}},
    #         {"source": {"name": "Call Type"}, "sink": {"name": "call_type"}},
    #         {"source": {"name": "Call Date"}, "sink": {"name": "call_date"}},
    #         {"source": {"name": "Watch Date"}, "sink": {"name": "watch_date"}},
    #         {"source": {"name": "Received DtTm"}, "sink": {"name": "received_date"}},
    #         {"source": {"name": "Entry DtTm"}, "sink": {"name": "entry_time"}},
    #         {"source": {"name": "Dispatch DtTm"}, "sink": {"name": "dispatch"}},
    #         {"source": {"name": "Response DtTm"}, "sink": {"name": "response_time"}},
    #         {"source": {"name": "On Scene DtTm"}, "sink": {"name": "on_scene"}},
    #         {"source": {"name": "Transport DtTm"}, "sink": {"name": "transport_time"}},
    #         {"source": {"name": "Hospital DtTm"}, "sink": {"name": "hospital_time"}},
    #         {"source": {"name": "Call Final Disposition"}, "sink": {"name": "final_dispatch"}},
    #         {"source": {"name": "Available DtTm"}, "sink": {"name": "avaiable_time"}},
    #         {"source": {"name": "Address"}, "sink": {"name": "address"}},
    #         {"source": {"name": "City"}, "sink": {"name": "city"}},
    #         {"source": {"name": "Zipcode of Incident"}, "sink": {"name": "zipcode"}},
    #         {"source": {"name": "Battalion"}, "sink": {"name": "batallion"}},
    #         {"source": {"name": "Station Area"}, "sink": {"name": "station_area"}},
    #         {"source": {"name": "Box"}, "sink": {"name": "box"}},
    #         {"source": {"name": "Original Priority"}, "sink": {"name": "origin_district"}},
    #         {"source": {"name": "Priority"}, "sink": {"name": "priority"}},
    #         {"source": {"name": "Final Priority"}, "sink": {"name": "final_priority"}},
    #         {"source": {"name": "ALS Unit"}, "sink": {"name": "als_unit"}},
    #         {"source": {"name": "Call Type Group"}, "sink": {"name": "call_type_group"}},
    #         {"source": {"name": "Number of Alarms"}, "sink": {"name": "num_alarms"}},
    #         # {"source": {"name": "Unit Type"}, "sink": {"name": "unit_type"}},
    #         {"source": {"name": "Unit sequence in call dispatch"}, "sink": {"name": "unit_seq"}},
    #         {"source": {"name": "Fire Prevention District"}, "sink": {"name": "fire_prevent_district"}},
    #         {"source": {"name": "Supervisor District"}, "sink": {"name": "supervisor_district"}},
    #         {"source": {"name": "Neighborhooods - Analysis Boundaries"}, "sink": {"name": "neighborhood"}},
    #         {"source": {"name": "RowID"}, "sink": {"name": "rowid"}},
    #         {"source": {"name": "case_location"}, "sink": {"name": "case_loc"}}
    #         # {"source": {"name": "Analysis Neighborhoods"}, "sink": {"name": "analysis_neighborhoods"}},

    #     ],
    # )
    synapse_sql_query = SynapseSQLOperator(
        task_id="synapse_sql_query",
        sql="SELECT zipcode, count(*) from [dbo].[sf_active_business_locations] GROUP BY zipcode",
    )

    wasb_to_synapse