import json

from airflow.decorators import dag, task_group, task
from airflow.operators.bash import BashOperator
from airflow.sensors.base import PokeReturnValue
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator, S3CopyObjectOperator, \
    S3CreateObjectOperator
from pendulum import datetime, duration

from datetime import datetime as dt

from utils.docIE import get_access_token, create_job, get_job_status

default_args = {
    "owner": "Anudeep"
}


@dag(dag_id="invoice2receipt",
     default_args=default_args,
     start_date=days_ago(1),
     schedule_interval=None)
def invoice2receipt():
    @task
    def read_invoices_from_S3():
        s3_hook = S3Hook("AWS_connection")
        print(s3_hook)
        paths = s3_hook.list_keys(bucket_name='invoices', prefix='')
        return paths

    @task_group(group_id="process_each_invoice")
    def process_invoice(filepath):
        @task
        def download_from_s3(key):
            hook = S3Hook('AWS_connection')
            file_path = hook.download_file(
                key=key,
                bucket_name="invoices",
                preserve_file_name=True
            )
            # will return absolute path
            return file_path

        @task
        def create_extraction_job(fpath, ti, **kwargs):
            token = get_access_token()
            ti.xcom_push(key="token", value=token)
            #{"status": "PENDING", "id": "4f2fa613-f3a3-4d28-b608-b98297fdd42c", "processedTime": "2024-06-12T03:45:15.977378+00:00"}
            job_response = create_job(token=get_access_token(), filepath=fpath)
            #job_response = {"id": "4f2fa613-f3a3-4d28-b608-b98297fdd42c"}
            return job_response["id"]

        @task.sensor(
            poke_interval=60,
            exponential_backoff=True,
            mode="reschedule"
        )
        def check_job_status(job_id, ti, **kwargs):
            token = ti.xcom_pull(key="token")
            inv_data = get_job_status(job_id=job_id[0], token=token[0])
            if inv_data["status"] == "DONE":
                condition_met = True
                operator_return_value = inv_data["extraction"]
            else:
                condition_met = False
                operator_return_value = None
                #raise Exception("Retry - Job is still pending")
            return PokeReturnValue(is_done=condition_met, xcom_value=operator_return_value)

        @task.bash()
        def clean_temp_file(path):
            temp_directory = path.split("/")[:-1]
            temp_directory = '/'.join(temp_directory)
            print(temp_directory)
            return f"rm -rf {temp_directory}"

        @task
        def get_invoice_data(data):
            print(data)
            return data

        temp_path = download_from_s3(filepath)
        job_id = create_extraction_job(temp_path),
        get_invoice_data(check_job_status(job_id)) >> clean_temp_file(temp_path)

    @task
    def write_data_to_S3(ti, **kwargs):
        data = ti.xcom_pull(task_ids=["process_each_invoice.print_data"], key="return_value")
        data = str(data)
        utc_time = dt.today().strftime('%Y-%m-%d_%H-%M-%S')
        add_data = S3CreateObjectOperator(
            task_id="write_to_s3",
            s3_bucket="invoice-json",
            s3_key=f"{utc_time}.json",
            aws_conn_id="AWS_connection",
            data=json.dumps(data)
        )
        print(data)
        add_data.execute(context=None)

    paths = read_invoices_from_S3()
    process_invoice_object = process_invoice.expand(filepath=paths)
    process_invoice_object >> write_data_to_S3()


invoice2receipt()
