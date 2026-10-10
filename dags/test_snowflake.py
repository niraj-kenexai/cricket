from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="test_snowflake_connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["snowflake", "test"],
) as dag:

    test_query = SQLExecuteQueryOperator(
        task_id="test_snowflake_ping",
        conn_id="snowflake_default",
        sql="""
        SELECT 
            CURRENT_VERSION() AS snowflake_version,
            CURRENT_ACCOUNT() AS account_name,
            CURRENT_ROLE() AS current_role,
            CURRENT_DATABASE() AS current_database,
            CURRENT_SCHEMA() AS current_schema,
            CURRENT_WAREHOUSE() AS current_warehouse;
        """,
    )
