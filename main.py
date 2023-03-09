"""
Program queries database for open Addepar API jobs. Depending on the status of the jobs, the program
will either POST the job to the Addepar Jobs API, query the status and download the job data, import
the data into the database by executing the job's import proc, or execute the post-import proc to
scrub the data in the dbimport table and move it to the target table.
"""

from configparser import ConfigParser
import logging.config
import base64
import os
import json
from datetime import datetime
import requests
import pymssql
import database_utils as dbutil
import addepar_params


def create_auth_string(key: str, secret: str, method='utf-8') -> str:
    """
    Create an authorization string per Addepar's specifications to access the Jobs API
    https://developers.addepar.com/docs/basic-authentication

    Args:
        key (str): API username
        secret (str): API secret
        method (str, optional): Variant of Base-64 encoding to use. Defaults to 'utf-8'.

    Returns:
        str: Authorization string that must be included in the jobs API request header
    """
    enc_bytes = base64.b64encode(bytes(key + ':' + secret, method))
    auth_string = 'Basic ' + enc_bytes.decode(method)
    return auth_string


def post_addepar_job(url: str, header: dict, params: str, response_save_path: str = '', timeout: int = 300) -> int:
    """
    Post a job to the Addepar API defined by the params variable
    https://developers.addepar.com/docs/jobs#create-a-portfolio-query-job
    https://developers.addepar.com/docs/basic-authentication

    Args:
        url (str): Addepar API endpoint URL
        header (dict): Authorization header dictionary described in the Basic Authentication
                section of Addepar's website
        params (str): JSON string defining the parameters of the API request
        response_save_path (str, optional): Directory to save the decoded API response.
                If left blank (''), the response will not be saved. Defaults to ''.
        timeout (int, optional): Wait time for the API before timing out. Defaults to 300 seconds.

    Returns:
        int: Addepar Jobs API job ID if the job was successfully posted and accepted.
                Returns None if an error was encountered.
    """

    # Post the Addepar API job
    try:
        response = requests.request('POST', url, headers=header, data=params, timeout=timeout)
        _str = f"Addepar job posted. HTTP response code = {response.status_code}"
        logger.info(_str)
    except requests.exceptions.RequestException as err:
        logger.error("Error encountered posting Addepar API job.")
        logger.error(err)
        return None

    # Decode the response
    json_str = response.content.decode('utf-8')

    # Write the API response to a text file if requested
    if response_save_path != '':
        # Build a complete save path
        response_save_path = os.path.join(response_save_path, '') + "AddeparPostResponse_"
        response_save_path += f"{datetime.now().strftime('%Y%m%d-%H%M%S%f')}.txt"
        api_response_to_file(json_str, response_save_path)

    # If the job was accepted, Addepar returns a 202 status code
    # If any other status code was returned, log an error
    if response.status_code != 202:
        logger.error("Unexpected response status code from Addepar Job API Post.")

        # If the content is less than 1000 bytes, print it to the log
        if len(response.content) <= 800:
            logger.info(json_str)
        else:
            logger.info("Entire response is too large to display. Partial response:")
            logger.info(json_str[0:800])

        return None

    # try to parse the decoded response string as JSON
    try:
        json_dict = json.loads(json_str)
    except json.JSONDecodeError as err:
        logger.error("Could not parse decoded API response to JSON.")
        logger.error(err)
        return None

    # Extract the Addepar API Job ID from the JSON dictionary
    try:
        job_id = json_dict['data']['id']
    except KeyError as err:
        logger.error("Could not parse the Addepar Job ID from the response string.")
        _log_str = f"JSON Dictionary Key Error: {err}"
        logger.error(_log_str)
        return None

    # If the job was successfully posted and the Job ID was extracted from the response, return it
    _log_str = f"API Job accepted by Addepar. Job ID = {job_id}"
    logger.info(_log_str)
    return job_id


def check_addepar_job_status(base_url: str, job_id: int, header: dict, timeout: int = 300) -> float:
    """
    Check the status of the specified Addepar API job. If the job is complete, download the data
    and save it.
    https://developers.addepar.com/docs/jobs#check-the-status-of-a-job

    **Once the job is complete, the response status code will change from 200 to 303.**
    The Addepar Jobs API status query will automatically redirect the request and download the job
    data once the job is complete. This can cause issues as the status response is <1KB but the data
    payload can be >100KB+. To avoid this, allow_redirects=False in the API query.

    Args:
        base_url (str): The base Addepar Jobs API URL
        job_id (int): The **Addepar** Jobs API ID of the job
        header (dict): Authorization header dictionary described in the Basic Authentication
                section of Addepar's website
        timeout (int, optional): Wait time for the API before timing out. Defaults to 300 seconds

    Returns:
        float: The completion percentage of the job
    """

    # Query the status of the Addepar Job from the API
    url = f'{base_url}/{job_id}'
    try:
        response = requests.request('GET', url, headers=header, allow_redirects=False, timeout=timeout)
        _log_str = f"Addepar Job {job_id} status queried. HTTP response code = {response.status_code}"
        logger.info(_log_str)
    except requests.exceptions.RequestException as err:
        _log_str = f"Error encountered querying the status Addepar API job ID = {job_id}."
        logger.error(_log_str)
        logger.error(err)
        return None

    # Decode the response
    json_str = response.content.decode('utf-8')

    # If the job status query was successful, Addepar will return a status code of 200
    # if the job is still in progress or 303 (redirection) if the job is complete.
    if not (response.status_code == 200 or response.status_code == 303):
        logger.error("Unexpected response status code from Addepar API Job status query.")

        # If the content is less than 800 bytes, print it to the log
        if len(response.content) <= 800:
            logger.info(json_str)
        else:
            logger.info('Entire response is too large to display. Partial response:')
            logger.info(json_str[0:800])

        return None

    # Parse the response to JSON
    try:
        json_dict = json.loads(json_str)
    except json.JSONDecodeError as err:
        logger.error("Could not parse the decoded API response to JSON.")
        logger.error(err)
        return None

    # Extract the Addepar API percent_complete attribute from the JSON dictionary
    try:
        addepar_pct_complete = json_dict['data']['attributes']['percent_complete']
        _log_str = f"Addepar Job {job_id} percent complete = {addepar_pct_complete} / 1.000"
        logger.info(_log_str)
    except KeyError as err:
        logger.error("Could not parse the percent_complete attribute from the response string.")
        _log_str = f"Key Error: {err}"
        logger.error(_log_str)

        return None

    # If no errors were encountered, return the percent complete
    return addepar_pct_complete


def download_addepar_job(base_url: str, job_id: int, header: dict, save_path: str, timeout: int = 300) -> bool:
    """
    Download a completed Addepar API Job
    https://developers.addepar.com/docs/jobs#download-the-results-of-a-job

    Args:
        base_url (str): The base Addepar Jobs API URL
        job_id (int): The **Addepar** Jobs API ID of the job
        header (dict): Authorization header dictionary described in the Basic Authentication
                section of Addepar's website
        save_path (str): full file path and name to save the returned API Job data
        timeout (int, optional): Wait time for the API before timing out. Defaults to 300 seconds

    Returns:
        bool: Success (True) or Failure (False) of downloading the job data
    """

    # Download the Addepar Job data from the API
    url = f'{base_url}/{job_id}/download'
    try:
        response = requests.request('GET', url, headers=header, timeout=timeout)
        _log_str = f"Addepar Job {job_id} job downloaded. HTTP response code = {response.status_code}"
        logger.info(_log_str)
    except requests.exceptions.RequestException as err:
        _log_str = f"Error encountered downloading the Addepar API Job ID = {job_id}."
        logger.error(_log_str)
        logger.error(err)
        return False

    # Decode the response
    json_str = response.content.decode('utf-8')

    # If the job data download was successful
    if response.status_code != 200:
        logger.error("Unexpected response status code from Addepar API Job data download.")

        # If the content is less than 800 bytes, print it to the log
        if len(response.content) <= 800:
            logger.info(json_str)
        else:
            logger.info('Entire response is too large to display. Partial response:')
            logger.info(json_str[0:800])

        return False

    # If the Addepar API Job data download was successful, save the data to the provided file path
    write_success = api_response_to_file(json_str, save_path)

    # If failed writing the JSON response to a file, return None indicating an error
    if not write_success:
        return False

    # If no errors occurred, return True denoting the job data was successfully downloaded and saved
    return True


def update_job_status_db(conn: pymssql.Connection, job_id: int, to_status: str, to_job_details: str) -> bool:
    """
    Update a specific job's status in the database JobQueue table using the
    Addepar.usp_UpdateJobQueueStatus stored procedure.
    *Job statuses should only be updated using this proc to ensure the audit data is also updated.

    Args:
        conn (pymssql.Connection): An open connection to a database server
        job_id (int): Unique **database** ID of the job to update the status of
        to_status (str): Status name to update the
        to_job_details (str): Job details value to update the database with
                To Posted: The Addepar API job ID
                To Downloaded: The file path of the JSON data
                To Imported: The number of rows imported into the dbimport table
                To Completed: The number of rows imported into the target table

    Returns:
        bool: True if job status was successfully updated, False if error occurred
    """

    _log_str = f"Updating Job ID = {job_id} status in database to '{to_status}'."
    logger.info(_log_str)

    # Build the SQL statement
    sql = f"EXEC Addepar.usp_UpdateJobQueueStatus @JobQueueIdToUpdate={job_id}, "
    sql += f"@JobDetails='{to_job_details}', @UpdateToStatusName='{to_status}'"

    cursor = None
    try:
        # Execute the SQL
        logger.debug(sql)
        cursor = conn.cursor()
        cursor.execute(sql)

        # The proc returns a success or failure boolean
        update_success = cursor.fetchone()[0]

        # Clean up
        cursor.close()
    except pymssql.Error as err:
        if cursor is not None:
            cursor.close()
        _log_str = f"SQL error encountered. Job ID = {job_id} was not updated."
        logger.error(_log_str)
        logger.error(err)
        return False

    if update_success == 1:
        # Status was successfully updated
        _log_str = f"Job ID = {job_id} status updated to '{to_status}' with details = {to_job_details}"
        logger.info(_log_str)
        return True
    else:
        # Failed to update job status
        _log_str = f"SQL error encountered. Job ID = {job_id} was not updated."
        logger.error(_log_str)
        return False


def api_response_to_file(content: str, file_path: str) -> bool:
    """
    Write the content of an API response to a text file

    Args:
        content (str): Decoded API response to write to a file
        file_path (str): File path to write save the response to

    Returns:
        bool: Success (True) or failure (False) of file write
    """

    try:
        # Write to file
        with open(file_path, 'w', encoding='utf-8') as outfile:
            outfile.write(content)

        _log_str = f"Wrote API response to file: {file_path}"
        logger.info(_log_str)
        return True

    except IOError as err:
        _log_str = f"IOError encountered: {err}"
        logger.error(_log_str)
        logger.error("Failed writing API response to file.")
        return False


def exec_import_proc(conn: pymssql.Connection, sql: str) -> int:
    """
    Execute a post import proc string returned by SQL. Return the number of rows processed.

    Args:
        conn (pymssql.Connection): An open connection to a database server
        sql (str): SQL EXEC string to execute

    Returns:
        int: Number of rows inserted. If an error was encountered, the method will return -1.
    """

    cursor = None
    try:
        # Execute the SQL
        logger.debug(sql)
        cursor = conn.cursor()
        cursor.execute(sql)

        # The proc returns a success or failure boolean
        _rows_inserted = cursor.fetchone()[0]

        # Clean up
        cursor.close()
    except pymssql.Error as err:
        if cursor is not None:
            cursor.close()
        logger.error("SQL error encountered executing proc. Data was not processed.")
        logger.error(err)
        return -1

    _log_str = f"{_rows_inserted} rows inserted."
    logger.info(_log_str)
    return _rows_inserted


def process_all_jobs(jobs: list, api_timeout: int = 300) -> bool:
    """
    Process all the open Addepar jobs and return if any of the open jobs can be rerun immediately,
    before stopping the programs execution. Jobs can only be reprocessed immediately after
    successful completion of either the download or import steps.

    **Note: as of 2023-01-18
    Currently no step is re-tried if it fails the first time. It is unknown how frequently posting
    or getting data from the Addepar API will fail. If failures are frequent and it is found that
    adding a retry fixes most failures, automatic retries will be added later.

    Args:
        jobs (list): open Addepar jobs with all the details required to complete the next process
        api_timeout (int, optional): Wait time for the API before timing out.
                Defaults to 300 seconds

    Return:
        bool: denoting if any of the Addepar jobs can be rerun immediately before exiting python.
    """

    # Initiate the _rerun flag variable
    _rerun = False

    # Loop through each open job
    for job in jobs:
        # Initialize the rerun flag for this particular job
        rerun_job = False

        # Unpack the SQL record dictionary returned by the Addepar.usp_GetOpenJobs proc
        db_job_id = job['ID']                   # Specific **database** ID of the job
        job_name = job['JobName']               # Name of the queued job ('Accounts' or 'Holdings')
        job_date = job['AsOfDate']              # Date as of which the job is being run
        job_status = job['StatusName']          # Current status of the job
        job_details = job['QueryParameters']    # Data required to complete the next step of the job
        # If job status is 'Queued' job_details is required Addepar API job parameters
        # If job status is 'Posted' job_details is Addepar Job ID of the API query
        # If job status is 'Downloaded' job_details is full file path where the API data was saved
        # If job status is 'Imported' job_details is number of rows imported to into the DB

        # Direct traffic based on the current job status
        _log_str = f"Processing Job ID = {db_job_id}, job type = {job_name}, "\
                   f"as of = {job_date}, status = {job_status}"
        logger.info(_log_str)
        match job_status:
            case 'Queued':
                # Job is queued, the Addepar API job needs to be posted
                addepar_job_id = post_addepar_job(BASE_URL, addepar_header, job_details,
                                                  response_save_path='logs/', timeout=api_timeout)

                # If the post_addepar_job returns None, an error was encountered
                # Otherwise, it returns the integer Addepar API Job ID of the posted job
                if addepar_job_id is None:
                    update_job_status_db(db_conn, db_job_id, 'Error',
                                         'Failure posting job to Addepar - see logs for details.')
                else:
                    update_job_status_db(db_conn, db_job_id, 'Posted', addepar_job_id)

            case 'Posted':
                # Addepar API job has been posted, check the status and download if complete
                job_pct_complete = check_addepar_job_status(BASE_URL, job_details, addepar_header, api_timeout)

                if job_pct_complete is None:
                    # Error was encountered checking the status of the job
                    update_job_status_db(db_conn, db_job_id, 'Error',
                                         'Failure downloading API data from Addepar - see logs for details.')

                elif job_pct_complete >= 0 and job_pct_complete < 1:
                    # No error check job status but job is not yet complete
                    logger.info("Addepar API job not yet complete. Taking no further action.")

                elif job_pct_complete == 1:
                    # Job was successfully completed download it now
                    logger.info("Addepar API job complete. Downloading it now.")
                    data_save_path = f'data/{job_name}_{job_date}.json'

                    if download_addepar_job(BASE_URL, job_details, addepar_header, data_save_path, api_timeout):
                        update_job_status_db(db_conn, db_job_id, 'Downloaded', data_save_path)

                        # If the job data was successfully downloaded, this job can be rerun again
                        rerun_job = True

                    else:
                        _log_str = f"Error downloading {job_name} data from Addepar for Job ID = {db_job_id}."
                        logger.error(_log_str)
                        update_job_status_db(db_conn, db_job_id, 'Error',
                                             'Error downloading API data from Addepar - see logs for details.')

                else:
                    # Somehow got a percent_complete <0 or >1  -->  Throw an error
                    _log_str = f"ValueError: percent_complete = {job_pct_complete}. Value must be between 0 and 1"
                    logger.error(_log_str)

            case 'Downloaded':
                # Addepar API job data has been downloaded, import the data into the dbimport table
                rows_inserted = exec_import_proc(db_conn, job_details)

                # If an error was encountered, the method will return -1
                if rows_inserted == -1:
                    _log_str = f"Error importing {job_name} data into the dbimport table for Job ID = {db_job_id}."
                    logger.error(_log_str)
                    update_job_status_db(db_conn, db_job_id, 'Error',
                                         'Failure importing data into dbimport table - see logs for details.')

                if rows_inserted >= 0:
                    _log_str = f"{job_name} data imported into dbimport table for Job ID = {db_job_id}."
                    logger.info(_log_str)
                    update_job_status_db(db_conn, db_job_id, 'Imported', rows_inserted)

                    # If the job data was successfully imported, this job can be rerun again
                    rerun_job = True

            case 'Imported':
                # Job data has been imported to the dbimport table, run the post import proc
                rows_inserted = exec_import_proc(db_conn, job_details)

                # If an error was encountered, the method will return -1
                if rows_inserted == -1:
                    _log_str = f"Error processing {job_name} data into the target table for Job ID = {db_job_id}."
                    logger.error(_log_str)
                    update_job_status_db(db_conn, db_job_id, 'Error',
                                         'Failure inserting data into target table - see logs for details.')

                if rows_inserted >= 0:
                    logger.info("Job data scrubbed and inserted into target table.")
                    update_job_status_db(db_conn, db_job_id, 'Completed', rows_inserted)

            case _:
                # Status is not matched - return an error
                _log_str = "Unexpected job status returned. Please add code to "\
                    f"handle the case where Job Status = {job_status}."
                logger.error(_log_str)

        _log_str = f"Processing Job ID = {db_job_id} complete.\n"
        logger.info(_log_str)
        # If this job can be rerun again, update the overall rerun flag
        _rerun = _rerun or rerun_job

    return _rerun


if __name__ == "__main__":
    # Set up logging
    config = ConfigParser()
    config.read("config.ini")
    project_name = config.get('environment', 'project')
    log_timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    logging.config.fileConfig(
        "config.ini",
        disable_existing_loggers=False,
        defaults={"logfilename": f"logs/{project_name}_{log_timestamp}.log"}
    )
    logger = logging.getLogger(__name__)

    # Get all the required parameters
    ############################################################################################
    # Get database information
    CONN_STR = os.environ.get('MSSQL_PYCONN_STRING')
    SQL = "EXEC Addepar.usp_GetOpenJobs"

    # Get Addepar API information
    ADDEPAR_KEY = os.environ.get('ADDEPAR_API.KEY')
    ADDEPAR_SECRET = os.environ.get('ADDEPAR_API.SECRET')
    API_TIMEOUT = int(config.get('addepar_api', 'timeout'))
    addepar_header = addepar_params.HEADER
    BASE_URL = addepar_params.BASE_URL
    addepar_auth_string = create_auth_string(ADDEPAR_KEY, ADDEPAR_SECRET)
    # Add the auth string to the API header
    addepar_header['Authorization'] = addepar_auth_string

    # Core of the program
    ############################################################################################
    # Get data on all the open jobs from the database
    rerun = True
    while rerun:
        # Get data on all the open jobs from the database
        db_conn = pymssql.connect(
            server='eu-az-sql-serv1.database.windows.net',
            database='dlavnozlvyw33no',
            user='ux0pv970bzf8zzx',
            password='lIbQnEIiHmV751!@2J7JOK2YO')
        logger.info("Connected to database")
        open_jobs = dbutil.query_to_list(db_conn, SQL)
        print(open_jobs)
        log_str = f"{len(open_jobs)} open Addepar Jobs\n"
        logger.info(log_str)

        rerun = process_all_jobs(open_jobs)

        db_conn.commit()
        db_conn.close()
        logger.info("Database connection closed.")

    logger.info("Execution complete. Exiting program.")
