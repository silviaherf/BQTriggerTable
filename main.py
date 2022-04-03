#Version 14/09 with 2CF and runtime environment variables

from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime, date
import google.cloud.logging_v2
import json
import os

"""
Go to BigQtrigger main function to understand used parameters
"""
def createTriggerTable(triggerDataTable):
    """
    This function will create a new table in BigQuery that will contain allocate the transformed data from the log files
    I.e.: tableName (from the target dataset) and updateDate. This table will trigger the Qlik dashboard to be reloaded

    """
    BQclient = bigquery.Client()

    try:
        table = bigquery.table.Table(triggerDataTable)
        BQclient.get_table(table)  
        return "Table {} already exists.".format(triggerDataTable)
    except:
        print("Table {} is not found.".format(triggerDataTable))

        schema = [
        bigquery.SchemaField("tableName", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("updateDate", "STRING", mode="REQUIRED"),
        ]
        
        table = bigquery.table.Table(triggerDataTable, schema=schema)
        createdtable = BQclient.create_table(table)  

        print("Creating trigger table")
        assert table.expires is None

        return "Created table {}.{}.{}".format(createdtable.project, createdtable.dataset_id, createdtable.table_id)


def createBucket(dataset):
    CSclient = storage.client.Client()

    # The name for the new bucket that will receive the logFiles
    bucketName=dataset
    logBucket = storage.bucket.Bucket(client=CSclient,name=bucketName)

    if logBucket.exists()==True:
        return "Bucket already existing"

    bucket = CSclient.create_bucket(bucket_or_name=logBucket, location='europe-west1')  

    print(f"Bucket {bucket.name} created.")

    createdBucket = CSclient.get_bucket(bucket_or_name =logBucket)
    createdBucket.add_lifecycle_delete_rule(age=1)
    createdBucket.patch()

    print("Lifecycle rule added to the bucket")

    return "Lifecycle rule added to the bucket"


def create_sink(sink_name, dataset, project_id):
    # Instantiates a client
    loggingClient = google.cloud.logging_v2.Client()
    BQclient = bigquery.Client()

    # Retrieves a Cloud Logging handler based on the environment
    # you're running in and integrates the handler with the
    # Python logging module. By default this captures all logs
    # at INFO level and higher
    loggingClient.get_default_handler()
    loggingClient.setup_logging()

    """Creates a sink to export logs to the given Cloud Storage destination_bucket.

    The filter determines which logs this sink matches and will be exported
    to the destination. For example a filter of 'severity>=INFO' will send
    all logs that have a severity of INFO or greater to the destination.
    See https://cloud.google.com/logging/docs/view/advanced_filters for more
    filter information.
    """
    dataset_ref=f"{project_id}.{dataset}"
    datasetBQ=bigquery.dataset.Dataset(dataset_ref)

    tablesList=BQclient.list_tables(dataset=datasetBQ)

    filter_=""
    tableFilter=""

    for table in list(tablesList):
        
        tableName=table.table_id

        if tableName != "updateInfo":
        
            tableFilter=f'protoPayload.resourceName="projects/{project_id}/datasets/{dataset}/tables/{tableName}" '

            filter_+=tableFilter

            filter_+=" OR "

    filter_=filter_[0:-3]
    filter_+=""" AND
    protoPayload.metadata."@type"="type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata"
    AND
    protoPayload.methodName="google.cloud.bigquery.v2.JobService.InsertJob"
    """

    print(filter_)

    # The destination can be a Cloud Storage bucket, a Cloud Pub/Sub topic,
    # or a BigQuery dataset. In this case, it is a Cloud Storage Bucket.
    # Once the sink is successfully created, it's CRUCIAL to assign "Storaje Object Creator" permissions to our sink writer SA in our Cloud Storage bucket console
    # See https://cloud.google.com/logging/docs/api/tasks/exporting-logs for
    # information on the destination format.
    # https://cloud.google.com/logging/docs/export


    destination_bucket=f"storage.googleapis.com/{dataset}"

    sink = loggingClient.sink(name=sink_name, filter_=filter_, destination=destination_bucket)

    if sink.exists():
        return "Sink {} already exists.".format(sink.name)

    sink.create(unique_writer_identity=True)
    return "Created sink {}".format(sink.name)  


def insertTriggerDate(triggerDataTable,dataset):
    loggingClient = google.cloud.logging_v2.client.Client()
    BQclient = bigquery.Client()
    CSclient = storage.client.Client()

    #We extract the data from the log files to our triggerDataTable in BigQ

    #https://cloud.google.com/bigquery/docs/reference/auditlogs#bigqueryauditmetadata_format
    #https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata
    #https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry

    bucketName=f'{dataset}'

    logBucket = storage.bucket.Bucket(client=CSclient,name=bucketName)

    blobList = list(CSclient.list_blobs(bucket_or_name=logBucket))

    print(f"Number of blobs in logBucket: {len(blobList)}")

    if len(blobList)>0:

        rows_to_insert=[]

        for blob in blobList:

            dataLogs = [line for line in blob.open("rt")]
            print(dataLogs[0])

            for jsonRaw in dataLogs:
                jsonObj=json.loads(jsonRaw)
                if jsonObj["protoPayload"]["authorizationInfo"][0]["permission"]=="bigquery.tables.updateData":
                    tableName=jsonObj["protoPayload"]["resourceName"].split("/")[-1]
                    logTimeString=jsonObj["receiveTimestamp"][0:-4]

                    logTime = datetime.strptime(logTimeString, '%Y-%m-%dT%H:%M:%S.%f').strftime("%Y%m%d %H%M%S")

                    rows_to_insert.append(
                    {"tableName": tableName, "updateDate": logTime})

        print(rows_to_insert)
       
        table = bigquery.table.Table(triggerDataTable)
        print("Trying to bulk the new info to the triggerDataTable")
        errors = BQclient.insert_rows_json(
            table=table, json_rows=rows_to_insert, row_ids=[None] * len(rows_to_insert)
        )
        
        if errors == []:
            print("Rows were added")
            return "New rows have been added."
        else:
            print("Encountered errors while inserting rows: {}".format(errors))
            return "Encountered errors while inserting rows: {}".format(errors)
    
    print("No logs were found in the logBucket")
    return "No logs were found in the logBucket"

def BQtrigger(*args):
    
    """
    We define the specific parameters for the function (these will be our initial input params in testing OR BETTER runtime environment variables)
    project_id: The name of the environment we're using on GCP. Runtime environment is created by GCP as "GCP_PROJECT", is not needed to define it again but only call it in our script
    dataset: The name of the dataset in BigQ

    These other parameters are built along the script from the previously mentioned input params:
    triggerDataTable: The new dataTable where we'll store latest updates Data
    factsDataTable: The dataTable we want to pursuit in real time
    sink_name: The name for the sink that will export log files from Cloud Logging into CS (BigQ also possible, but more expensive)
    destination_bucket: The name for the bucket in CS that will store the log files comming from Cloud Logging
    """
    
    try:
        request_json = request.get_json()        
        if request_json['project_id'] and request_json['dataset']:
            project_id=request_json['project_id']
            dataset=request_json['dataset']

            triggerDataTable = f"{project_id}.{dataset}.updateInfo"
            
            sink_name="updateDataTableSink"

            createTriggerTable(triggerDataTable)

            createBucket(dataset)

            create_sink(sink_name, dataset, project_id)
        
    except:
        project_id=os.environ.get('PROJECT_ID')
        dataset=os.environ.get('TARGET_DATASET')

        triggerDataTable = f"{project_id}.{dataset}.updateInfo"
            
        sink_name="updateDataTableSink"

        createTriggerTable(triggerDataTable)

        createBucket(dataset)

        create_sink(sink_name, dataset, project_id)

    return "Trigger table, logBucket and and logSink successfully created"

    
