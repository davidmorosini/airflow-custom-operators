"""This module contains Google BigQuery to Google Cloud Storage operator."""
from typing import Any, Dict, List, Optional, Sequence, Union
import json

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook


class BigQueryToGCSOperator(BaseOperator):
    """
    Transfers a BigQuery table to a Google Cloud Storage bucket.
    .. seealso::
        For more details about these parameters:
        https://cloud.google.com/bigquery/docs/reference/v2/jobs
    :param source_project_dataset_table: The dotted
        ``(<project>.|<project>:)<dataset>.<table>`` BigQuery table to use as the
        source data. If ``<project>`` is not included, project will be the project
        defined in the connection json. (templated)
    :param destination_cloud_storage_uris: The destination Google Cloud
        Storage URI (e.g. gs://some-bucket/some-file.txt). (templated) Follows
        convention defined here:
        https://cloud.google.com/bigquery/exporting-data-from-bigquery#exportingmultiple
    :param compression: Type of compression to use.
    :param export_format: File format to export.
    :param field_delimiter: The delimiter to use when extracting to a CSV.
    :param print_header: Whether to print a header for a CSV file extract.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param labels: a dictionary containing labels for the job/query,
        passed to BigQuery
    :param location: The location used for the operation.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "source_project_dataset_table",
        "destination_cloud_storage_uris",
        "labels",
        "impersonation_chain",
        "temporary_table",
        "sql",
        "schema_file_name"
    )
    template_ext: Sequence[str] = ()
    ui_color = "#e4e6f0"

    def __init__(
        self,
        sql: str,
        destination_cloud_storage_uris: List[str],
        temporary_dataset: str,
        temporary_table: str,
        compression: str = "NONE",
        export_format: str = "CSV",
        field_delimiter: str = ",",
        print_header: bool = True,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        labels: Optional[Dict] = None,
        location: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        use_legacy_sql: bool = False,
        maximum_bytes_billed: int = None,
        delete_temporary_table: bool = True,
        schema_bucket: str = None,
        schema_filename: str = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.sql = sql
        self.destination_cloud_storage_uris = destination_cloud_storage_uris
        self.temporary_dataset = temporary_dataset
        self.temporary_table = temporary_table
        self.compression = compression
        self.export_format = export_format
        self.field_delimiter = field_delimiter
        self.print_header = print_header
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.labels = labels
        self.location = location
        self.impersonation_chain = impersonation_chain
        self.use_legacy_sql = use_legacy_sql
        self.maximum_bytes_billed = maximum_bytes_billed
        self.delete_temporary_table = delete_temporary_table
        self.schema_bucket = schema_bucket
        self.schema_file_name = schema_filename 

    def execute(self, context):
        self.log.info("Starting the extraction")
        bigquery_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )

        temporary_table_id = f"{bigquery_hook.project_id}.{self.temporary_dataset}.{self.temporary_table}"
        self.log.info(f"Temporary table id `{temporary_table_id}`")

        temporary_source_table = {
            "projectId": bigquery_hook.project_id,
            "datasetId": self.temporary_dataset,
            "tableId": self.temporary_table,
        }

        save_query_configuration: Dict[str, Any] = {
            "query": {
                    "query": self.sql,
                    "destinationTable": temporary_source_table,
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE",
                    "useLegacySql": self.use_legacy_sql,
                }
        }

        if self.maximum_bytes_billed and self.maximum_bytes_billed > 0:
            save_query_configuration["query"]["maximumBytesBilled"] = self.maximum_bytes_billed

        self.log.info(f"Quering and send to temporary table `{temporary_table_id}`")
        bigquery_hook.insert_job(configuration=save_query_configuration)

        # extract data in to a bucket
        configuration: Dict[str, Any] = {
            "extract": {
                "sourceTable": temporary_source_table,
                "compression": self.compression,
                "destinationUris": self.destination_cloud_storage_uris,
                "destinationFormat": self.export_format,
            }
        }

        if self.labels:
            configuration["labels"] = self.labels

        if self.export_format == "CSV":
            # Only set fieldDelimiter and printHeader fields if using CSV.
            # Google does not like it if you set these fields for other export
            # formats.
            configuration["extract"]["fieldDelimiter"] = self.field_delimiter
            configuration["extract"]["printHeader"] = self.print_header

        self.log.info(f"Exporting data to GCS URIs `{self.destination_cloud_storage_uris}`")
        bigquery_hook.insert_job(configuration=configuration)

        if self.schema_save:
            self.log.info(f"Saving schema in `gs://{self.schema_bucket}/{self.schema_file_name}`")
            schema = bigquery_hook.get_schema(dataset_id=self.temporary_dataset, table_id=self.temporary_table, project_id=bigquery_hook.project_id)
            gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
            gcs_hook.upload(
                bucket_name=self.schema_bucket,
                object_name=self.schema_file_name,
                data=json.dumps(schema["fields"]),
                mime_type="application/json",
            )

        if self.delete_temporary_table:
            self.log.info(f"Deleting temporary table `{temporary_table_id}`")
            bigquery_hook.delete_table(table_id=temporary_table_id, project_id=bigquery_hook.project_id)
