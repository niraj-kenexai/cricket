from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

try:
    from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
    COSMOS_AVAILABLE = True
except ImportError:
    COSMOS_AVAILABLE = False

INPUT_FOLDER = Path("/usr/local/airflow/data/filepath")
DBT_PROJECT_PATH = "/usr/local/airflow/dbt_project"
DBT_PROFILES_PATH = "/usr/local/airflow/dbt_project/profiles.yml"
SNOWFLAKE_CONN_ID = "snowflake_default"


def check_for_csv():
    """
    Checks if there are any CSV files in INPUT_FOLDER that have not yet been successfully loaded into Snowflake.
    Prevents the sensor from falsely triggering repeatedly for already-loaded files.
    """
    files = list(INPUT_FOLDER.glob("*.csv"))
    if not files:
        print("No CSV files found in input directory.")
        return False

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    pending_files = []
    for file in files:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM BRONZE.FILE_AUDIT
            WHERE FILENAME = %s AND STATUS = 'SUCCESS'
            """,
            (file.name,)
        )
        already_loaded = cursor.fetchone()[0]
        if already_loaded == 0:
            pending_files.append(file.name)

    cursor.close()
    conn.close()

    if pending_files:
        print(f"Pending CSV files found to process: {pending_files}")
        return True

    print(f"All {len(files)} CSV files in directory have already been loaded.")
    return False


def load_files_to_bronze():
    """
    Performs high-performance ingestion of new CSV files into Snowflake BRONZE.RAW_DATA
    using Snowflake internal stage (PUT + COPY INTO).
    """
    files = list(INPUT_FOLDER.glob("*.csv"))
    if not files:
        print("No CSV files found to process.")
        return

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    for file in files:
        file_name = file.name
        print(f"Checking status for: {file_name}")

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM BRONZE.FILE_AUDIT
            WHERE FILENAME = %s AND STATUS = 'SUCCESS'
            """,
            (file_name,)
        )
        if cursor.fetchone()[0] > 0:
            print(f"Skipping {file_name} - already loaded.")
            continue

        print(f"Staging and loading {file_name} into Snowflake BRONZE.RAW_DATA...")
        file_posix_path = file.resolve().as_posix()

        try:
            put_sql = f"PUT 'file://{file_posix_path}' @BRONZE.CRICKET_STAGE AUTO_COMPRESS=TRUE OVERWRITE=FALSE PARALLEL=4"
            print(f"Executing PUT: {put_sql}")
            cursor.execute(put_sql)
            safe_file_name = file_name.replace("'", "''")
            copy_sql = f"""
            COPY INTO BRONZE.RAW_DATA (
                delivery_id, delivery_timestamp, innings_no, over_no, ball_in_over, runs_off_bat,
                extra_type, extra_runs, total_runs, is_wicket, dismissal_type, ball_speed_kmph,
                non_striker_id, shot_type, shot_region, match_id, match_date, match_format,
                tournament_name, season, match_stage, toss_decision, venue_id, venue_name,
                venue_city, venue_country, batting_team_id, batting_team_name, bowling_team_id,
                bowling_team_name, batsman_id, batsman_name, batsman_dob, batsman_nationality,
                batting_hand, batsman_role, batsman_email, batsman_phone, batsman_icc_rating,
                bowler_id, bowler_name, bowler_nationality, bowling_style, bowler_icc_rating,
                contract_id, contract_grade, annual_retainer_usd, contract_status, drs_review_id,
                drs_reviewed_by, drs_original_decision, drs_outcome, SOURCEFILE
            )
            FROM (
                SELECT 
                    t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                    t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
                    t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
                    t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
                    t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50, 
                    t.$51, t.$52, '{safe_file_name}'
                FROM @BRONZE.CRICKET_STAGE/{safe_file_name} t
            )
            FILE_FORMAT = (FORMAT_NAME = 'BRONZE.CSV_FILE_FORMAT')
            ON_ERROR = 'CONTINUE';
            """
            cursor.execute(copy_sql)
            copy_results = cursor.fetchall()
            print(f"COPY results: {copy_results}")

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM BRONZE.RAW_DATA
                WHERE SOURCEFILE = %s
                """,
                (file_name,)
            )
            rows_loaded = cursor.fetchone()[0]

            cursor.execute(
                """
                INSERT INTO BRONZE.FILE_AUDIT (FILENAME, ROWSLOADED, STATUS)
                VALUES (%s, %s, %s)
                """,
                (file_name, rows_loaded, "SUCCESS")
            )
            conn.commit()
            print(f"Successfully loaded {rows_loaded} rows from {file_name}.")

        except Exception as e:
            conn.rollback()
            cursor.execute(
                """
                INSERT INTO BRONZE.FILE_AUDIT (FILENAME, ROWSLOADED, STATUS)
                VALUES (%s, %s, %s)
                """,
                (file_name, 0, f"FAILED: {str(e)[:450]}")
            )
            conn.commit()
            print(f"Failed to load {file_name}: {e}")
            raise e

    cursor.close()
    conn.close()


with DAG(
    dag_id="cricket_raw_ingestion",
    start_date=datetime(2026, 9, 12),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    
) as dag:
    wait_for_csv = PythonSensor(
        task_id="wait_for_csv",
        python_callable=check_for_csv,
        poke_interval=30,
        timeout=3600,
        mode="reschedule",
    )

    load_to_bronze = PythonOperator(
        task_id="load_to_bronze",
        python_callable=load_files_to_bronze,
    )

    if COSMOS_AVAILABLE:
        profile_config = ProfileConfig(
            profile_name="capstone_dbt",
            target_name="dev",
            profiles_yml_filepath=DBT_PROFILES_PATH,
        )
        dbt_transform = DbtTaskGroup(
            group_id="dbt_transformations",
            project_config=ProjectConfig(
                dbt_project_path=DBT_PROJECT_PATH,
            ),
            profile_config=profile_config,
            render_config=RenderConfig(
                select=["path:models/gold"],
            ),
        )
    else:
        dbt_transform = BashOperator(
            task_id="dbt_transformations",
            bash_command=f"cd {DBT_PROJECT_PATH} && dbt run --select path:models/gold --profiles-dir {DBT_PROJECT_PATH}",
        )

    wait_for_csv >> load_to_bronze >> dbt_transform
