# Sparkify Airflow Project

## Description

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.
 
## Running the project.

### Required Airflow Connections

* The project requires a running Redshift cluster. 
* The DAG requires a Postgres connection called `redshift` which points at your active cluster.

* The project also requires an Amazon Web Services connection called `aws_credentials` with the access key and secret of AWS user with Redshift admin access.


### Redshift Tables
* Once the cluster is running, using the query editor you should run the sql in the `create_tables.sql`file to set up  all required tables.

### Airflow

* The file `sparkify_dag.py`is an Airflow DAG. Assuming you are running  Airflow and the DAG is loaded correctly.
Once the Connections and tables have been created, the DAG can be run.

* The DAG has the following structure.

    ![DAG](/images/graph.png)

## Notes

 * There is a data quality check on the artists table. If any rows are missing the `artistid` field, the process will fail.