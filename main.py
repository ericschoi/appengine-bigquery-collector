import datetime
import uuid
import simplejson

from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
from six.moves.urllib.error import HTTPError

from flask import Flask
from flask import request

app = Flask(__name__)
app.secret_key = "7W9YZ75HHZZR6RB8"
app.config['DEBUG'] = True


credentials = GoogleCredentials.get_application_default()
bigquery = build('bigquery', 'v2', credentials=credentials)


@app.route('/')
def index():
    """Return a friendly HTTP greeting."""
    return "Event Collector v1.0.1"


# @app.route('/e/<project_id>/<dataset_id>', methods=["GET"])
# def list_tables(project_id, dataset_id):
#     """List dataset's tables."""
#     return "%r" % bigquery.datasets().list(projectId=project_id).execute()


@app.route('/e/<project_id>/<dataset_id>/<table_id>', methods=["POST"])
def insert_event(project_id, dataset_id, table_id):
    """Insert events."""
    suffix = datetime.date.today().strftime("%Y%m%d")

    data = []
    rows = [request.json] if type(request.json) == type(dict()) else request.json
    for row in rows:
        insert_id = row["_id"]["$oid"]
        event_type = row["event_type"]
        player_id = row["player_id"]
        ts = row["ts"]
        body = simplejson.dumps(row)
        data.append({
            "json": {
                "event_type": event_type,
                "player_id": player_id,
                "timestamp": ts,
                "data": body
            },
            "insertId": insert_id
        })
    res = bigquery.tabledata().insertAll(
        projectId=project_id,
        datasetId=dataset_id,
        # datasetId=dataset_id,
        tableId=table_id,
        body={
            "rows": data,
            "templateSuffix": suffix
        }).execute()
    return "%r" % res
