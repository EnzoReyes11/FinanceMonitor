import os

import functions_framework
from dotenv import load_dotenv
from google.cloud.bigquery_storage_v1 import BigQueryWriteClient, types, writer
from google.cloud.bigquery_storage_v1.types import ProtoSchema
from google.protobuf import descriptor_pb2

load_dotenv()

try:
    write_client = BigQueryWriteClient()
except Exception as e:
    print(f"Critical: Error initializing BigQueryWriteClient: {e}")
    write_client = None


@functions_framework.http
def bq_storage_write_batch(request):
    """
    HTTP Cloud Function to batch insert data into BigQuery using Storage Write API.
    Expects a POST request with a JSON list of records:
    [{"symbol": "str", "value": int (for now), "datetime": "iso_str", "market": "str"}, ...]
    """
    if not write_client:
        print("Error: BigQueryWriteClient not initialized during startup.")
        return "Internal server error: BigQuery client unavailable", 500

    if request.method != "POST":
        return "Only POST requests are accepted", 405

    PROJECT_ID = os.getenv("BQ_PROJECT")
    DATASET_ID = os.getenv("BQ_DATASET")
    TABLE_ID = os.getenv("BQ_TABLE")

    if not all([PROJECT_ID, DATASET_ID, TABLE_ID]):
        print(
            "Error: Missing environment variables (PROJECT_ID, DATASET_ID, TABLE_ID)."
        )
        return "Server configuration error: Missing BigQuery identifiers", 500

    # 1. Define the ProtoSchema for the data
    # This must match the BigQuery table schema.
    # For BQ DATETIME, we'll use TYPE_STRINGj
    # For BQ FLOAT64/NUMERIC, we'll use TYPE_FLOAT.
    row_descriptor = descriptor_pb2.DescriptorProto()
    row_descriptor.name = "DataRecord"

    field_symbol = row_descriptor.field.add()
    field_symbol.name = "symbol"
    field_symbol.number = 1
    field_symbol.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    field_symbol.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING

    field_value = row_descriptor.field.add()
    field_value.name = "value"
    field_value.number = 2
    field_value.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    field_value.type = descriptor_pb2.FieldDescriptorProto.TYPE_FLOAT

    field_datetime = row_descriptor.field.add()
    field_datetime.name = "datetime"
    field_datetime.number = 3
    field_datetime.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    field_datetime.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING

    field_market = row_descriptor.field.add()
    field_market.name = "market"
    field_market.number = 4
    field_market.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    field_market.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING

    proto_schema = ProtoSchema()
    proto_schema.proto_descriptor.CopyFrom(row_descriptor)

    # 2. Serialize input data to Protobuf format
    # We use the library's internal _DictToProtoSerializer for convenience.
    # This avoids needing pre-compiled .proto files for this dynamic schema.
    proto_serializer = writer._DictToProtoSerializer(row_descriptor)
    serialized_rows = []
    import random

    random_integer = random.randint(1, 1000)
    try:
        processed_record = {
            "symbol": "META",
            "value": random_integer,
            "datetime": "2025-06-08T16:42:31.190280",
            "market": "US",
        }
        serialized_rows.append(proto_serializer.serialize(processed_record))
    except (ValueError, TypeError) as e:
        print("Skipping record due to processing error %s", e)

    if not serialized_rows:
        return "No valid records to insert after processing input.", 400

    # 3. Use BigQuery Storage Write API in Batch (PENDING stream) mode
    parent_table_path = write_client.table_path(PROJECT_ID, DATASET_ID, TABLE_ID)
    stream_name = None

    try:
        write_stream = types.WriteStream(type_=types.WriteStream.Type.PENDING)
        created_stream = write_client.create_write_stream(
            parent=parent_table_path, write_stream=write_stream
        )
        stream_name = created_stream.name
        print(f"Created PENDING write stream: {stream_name}")

        # Prepare the request to append rows
        append_request = types.AppendRowsRequest()
        append_request.write_stream = stream_name

        proto_data = types.ProtoData()
        proto_data.writer_schema.CopyFrom(proto_schema)
        proto_data.rows.serialized_rows.extend(serialized_rows)
        append_request.proto_rows = proto_data

        # Send data to the stream. For PENDING streams, this is one call with all data.
        # client.append_rows returns a future; .result() waits for completion.
        append_future = write_client.append_rows(iter([append_request]))
        append_response = (
            append_future.result()
        )  # Wait for the append operation to complete

        if append_response.HasField("error"):
            raise Exception(
                f"Error appending rows to stream '{stream_name}': {append_response.error.message}"
            )

        print(f"Successfully appended data to stream {stream_name}.")

        # Finalize the stream to indicate no more data will be written
        write_client.finalize_write_stream(name=stream_name)
        print(f"Finalized write stream: {stream_name}")

        # Commit the stream to make data visible
        commit_request = types.BatchCommitWriteStreamsRequest(
            parent=parent_table_path, write_streams=[stream_name]
        )
        commit_response = write_client.batch_commit_write_streams(
            request=commit_request
        )

        if commit_response.stream_errors:
            error_details = [
                f"Stream {err.write_stream}: {err.error_status.message}"
                for err in commit_response.stream_errors
            ]
            raise Exception(f"Errors during batch commit: {'; '.join(error_details)}")

        commit_time_str = (
            commit_response.commit_time.ToDatetime().isoformat()
            if commit_response.HasField("commit_time")
            else "N/A"
        )
        print(
            f"Batch successfully committed for stream {stream_name} at {commit_time_str}."
        )
        return (
            f"Data ({len(serialized_rows)} records) batch-loaded successfully into BigQuery.",
            200,
        )

    except Exception as e:
        print(f"An error occurred during BigQuery Storage Write operation: {e}")
        # Attempt to clean up the stream if it was created
        if stream_name:
            try:
                print(f"Attempting to finalize stream {stream_name} after error...")
                # Finalizing is generally safe. Aborting could also be an option
                # if the API supports AbortWriteStream and it's appropriate.
                write_client.finalize_write_stream(name=stream_name)
            except Exception as cleanup_e:
                print(
                    f"Failed to finalize stream {stream_name} during error cleanup: {cleanup_e}"
                )

        import traceback

        traceback.print_exc()  # Log full traceback for easier debugging
        return f"An internal error occurred: {str(e)}", 500
