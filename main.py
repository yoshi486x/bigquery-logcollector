import argparse
import datetime
import os
import requests
import subprocess
import sys
import time

from google.cloud import bigquery
from logging import getLogger, StreamHandler, DEBUG, INFO


# Settings for logging
logger = getLogger(__name__)
handler = StreamHandler()
handler.setLevel(INFO)
logger.setLevel(INFO)
logger.addHandler(handler)
logger.propagate = False


class JobStatistics2(object):
    def __init__(self, queryPlan=None, totalBytesBilled=0, billingTier=None, cacheHit=None, *args, **kwargs):
        self.queryPlan = queryPlan
        self.totalBytesBilled = int(totalBytesBilled)
        self.billingTier = billingTier
        self.cacheHit = cacheHit

        billed_cost = 0.0
        if totalBytesBilled:
            billed_cost = self.calculate_billed_cost(int(totalBytesBilled))
        self.billed_cost = billed_cost

    @classmethod
    def calculate_billed_cost(cls, totalBytesBilled, currency_type='USD', region='asia-northeast1') -> float:
        """$6.00 per TB on Tokyo Region"""
        if region == 'asia-northeast1':
            per_terabyte_cost = 6.0
        elif region == 'us-west1':
            per_terabyte_cost = 5.0
        return round(per_terabyte_cost * totalBytesBilled / 2**40, 2)


class JobStatistics(object):
    def __init__(self, creationTime=None, startTime=None, endTime=None, query=None, *args, **kwargs):
        self.creationTime = int(creationTime)
        self.startTime = int(startTime)
        self.endTime = int(endTime)
        if query:
            query = JobStatistics2(**query)
        else:
            query = JobStatistics2()
        self.query = query

        self.creation_time = self.micro_epochtime_to_isoformat(creationTime)

    @classmethod
    def micro_epochtime_to_isoformat(cls, micro_epochtime: int) -> str:
        microseconds = str(micro_epochtime)[-3:]
        epochtime = int(str(micro_epochtime)[:-3])
        return '.'.join([datetime.datetime.utcfromtimestamp(epochtime).isoformat(), microseconds])


class JobConfiguration(object):
    """https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfiguration"""

    def __init__(self, query=None, load=None):
        self.query = query
        self.load = load


class Job(object):
    """https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/list#response-body"""

    def __init__(self, id=None, kind=None, jobReference=None, state=None, errorResult=None, statistics=None, configuration=None, status=None, user_email=None):
        self.id = id
        self.kind = kind
        self.jobReference = jobReference
        self.state = state
        self.errorResult = errorResult
        self.statistics = JobStatistics(**statistics)
        self.configuration = self._set_configuration(**configuration)
        self.status = status
        self.user_email = user_email

    def _set_configuration(self, **configuration):
        # print('config:', configuration)
        query, load = None, None
        if 'query' in configuration.keys():
            query = configuration['query']
        if 'load' in configuration.keys():
            load = configuration['load']
        return JobConfiguration(query, load)


class BigQuery(object):
    project = '<YOUR_PROJECT_ID>'
    dataset_id = 'bigquery_log'

    # https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema.FIELDS.mode
    columns = ['logged_at', 'logged_epoch', 'state', 'query', 'user_email', 'total_bytes_billed', 'billed_cost', 'created_at']
    schema = [
        bigquery.SchemaField('logged_at', 'DATETIME', mode='NULLABLE'),
        bigquery.SchemaField('logged_epoch', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('state', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('query', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('user_email', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('total_bytes_billed', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('billed_cost', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('created_at', 'DATETIME', mode='REQUIRED'),
    ]
    base_query = "SELECT min(logged_epoch) AS oldest_epoch FROM `{table_id}`"

    def __init__(self, table_id=None, append=False, dryrun=False, table_name=None):
        self.client = bigquery.Client()
        if not table_id:
            table_id = self.generate_table_id(self.project, self.dataset_id, table_name)
            if not dryrun:
                self._create_table(table_id)
        self.table_id = table_id
        self.append = append

        self.query = self.base_query.format(table_id=table_id)

    @classmethod
    def generate_table_id(cls, project=None, dataset_id=None, table_name=None):
        if not table_name:
            prefix = 'log'
            d = datetime.datetime.utcnow()
            table_name = prefix + '-' + d.strftime('%Y%m%d%H%M00')
        return '.'.join([project, dataset_id, table_name])

    def _create_table(self, table_id=None):
        table = bigquery.Table(table_id, schema=self.schema)
        table = self.client.create_table(table)
        logger.info("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
        time.sleep(2)

    def insert_record(self, *rows_to_insert):
        print(f'========   Inserting rows into {self.table_id}   ========')
        errors = self.client.insert_rows_json(
            self.table_id, rows_to_insert, row_ids=[None] * len(rows_to_insert)
        )  # Make an API request.
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

    def select_oldest_timestamp(self):
        query_job = self.client.query(self.query)
        print("The query data:")
        try:
            for row in query_job:
                logger.info("timestamp={}".format(row[0]))
                return int(row[0]) - 1
        except:
            return None


class BigQueryLogCollector(BigQuery):
    def __init__(self, append=False, dryrun=False, infinite=False, table_id=None, maxCreationTime=None):
        super().__init__(table_id, append, dryrun)
        self.dryrun = dryrun
        self.maxCreationTime = maxCreationTime

        self._execute_handler(infinite)

    def _get_jobs_list(self, until: int, count=10000):
        payload = {'allUsers': 'true', 'maxCreationTime': until, 'maxResult': count, 'projection': 'full'}
        url = f'https://bigquery.googleapis.com/bigquery/v2/projects/{BigQuery.project}/jobs'
        access_token = os.environ.get('ACCESS_TOKEN')
        headers = {'Authorization': 'Bearer {}'.format(str(access_token)), 'Accept': 'application/json'}
        logger.debug({'access_token': access_token, 'url': url, 'headers': headers})
        return requests.get(url, params=payload, headers=headers)

    def _execute(self):
        # Set oldest timestamp
        if self.maxCreationTime:
            oldest_timestamp = self.maxCreationTime
        elif self.append:
            oldest_timestamp = self.select_oldest_timestamp()
        else:
            oldest_timestamp = int(time.time() * 1000)

        # Hit jobs.list API
        try:
            r = self._get_jobs_list(oldest_timestamp)
            r.raise_for_status()
        except requests.exceptions.HTTPError as err:
            logger.error({'status_code': r.status_code})
            print('Reset ACCESS_TOKEN if necessary. `$ export ACCESS_TOKEN="$(gcloud auth application-default print-access-token)"`')
            raise SystemExit(err)
        else:
            logger.info({'status_code': r.status_code})

        # Handle response
        response = r.json()
        logger.debug({'etag': response['etag'], 'kind': response['kind'], 'nextPageToken': response['nextPageToken']})
        jobs_list = response['jobs']
        records = []
        created_at = datetime.datetime.utcnow().isoformat()
        for j in jobs_list:
            job = Job(**j)
            oldest_timestamp = job.statistics.creationTime
            if job.configuration.query:
                values = [job.statistics.creation_time, job.statistics.creationTime, job.state, job.configuration.query['query'], 
                            job.user_email, job.statistics.query.totalBytesBilled, float(job.statistics.query.billed_cost), created_at]
                record = dict(zip(self.columns, values))
                records.append(record)
                print(job.statistics.creation_time, '{:4.2f}'.format(float(job.statistics.query.billed_cost)), job.user_email)
        if len(records) > 0 and not self.dryrun:
            self.insert_record(*records)

        return {'last_creation_time': oldest_timestamp}

    def _execute_handler(self, is_infinite):
        while True:
            stats = self._execute()
            if not is_infinite:
                break
            self.maxCreationTime = stats['last_creation_time'] - 1
            time.sleep(1)

    def __del__(self):
        print('\nCleaning up BigQueryLogCollector\n')


def command():
    # Configure command line option
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--append', help='look at oldest timestamp to avoid duplicate', action='store_true')
    parser.add_argument('-d', '--dryrun', help='do not insert in table', action='store_true')
    parser.add_argument('-i', '--infinite', help='loop until the end', action='store_true')
    parser.add_argument('-t', '--table_id', type=str, help='use given table_id')
    parser.add_argument('-M', '--maxCreationTime', type=int, help='use given table_id')
    args = parser.parse_args()
    print('args:', args)

    # Execute BigQueryLogColletor
    try:
        BigQueryLogCollector(args.append, args.dryrun, args.infinite, args.table_id, args.maxCreationTime)
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)


if __name__ == "__main__":
    """
    Set access-token before running this command
    $ export ACCESS_TOKEN="$(gcloud auth application-default print-access-token)"
    """
    command()
