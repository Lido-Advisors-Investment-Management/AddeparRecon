#!/usr/bin/env python3
from configparser import ConfigParser
import logging.config
import addepar_params
import database_utils as dbutil
import pyodbc
import requests
import base64
import os
from datetime import datetime
import json

def create_auth_string(key, secret, method='utf-8') -> str:
    """
    Create an authorization string per Addepar's sepcifications to access the Jobs API
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

def post_addepar_job(url, header, params, response_save_path='') -> int:
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

    Returns:
        int: Addepar Jobs API job ID if the job was successfully posted and accepted. 
                Returns None if an error was encountered.
    """
    
    # Post the Addepar API job
    try:
        r = requests.request('POST', url, headers=header, data=params)
        logger.info(f"Addepar job posted. HTTP response code = {r.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error encountered posting Addepar API job.")
        logger.error(e)
        return None
    
    # Decode the response
    json_str = r.content.decode('utf-8')
    
    # Write the API response to a text file if requested
    if response_save_path != '':
        # Build a complete save path
        response_save_path = os.path.join(response_save_path, '')
        response_save_path += f"AddeparPostResponse_{datetime.now().strftime('%Y%m%d-%H%M%S%f')}.txt"
        api_response_to_file(json_str, response_save_path)
    
    # If the job was accepted, Addepar returns a 202 status code
    # If any other status code was returned, log an error
    if r.status_code != 202:
        logger.error(f"Unexpected response status code from Addepar Job API Post.")
        
        # If the content is less than 1000 bytes, print it to the log
        if len(r.content) <= 800:
            logger.info(json_str)
        else: 
            logger.info('Entire response is too large to display. Partial response:')
            logger.info(json_str[0:800])
            
        return None
    
    # try to parse the decoded response string as JSON
    try:
        json_dict = json.loads(json_str)
    except json.JSONDecodeError as e:
        logger.error("Could not parse decoded API response to JSON.")
        logger.error(e)
        return None
    
    # Extract the Addepar API Job ID from the JSON dictionary
    try:
        addepar_job_id = json_dict['data']['id']
    except KeyError as e:
        logger.error("Could not parse the Addepar Job ID from the response string.")
        logger.error(f"JSON Dictionary Key Error: {e}")
        return None
    
    # If the job was successfully posted and the Job ID was extracted from the reponse, return it
    logger.info(f"API Job accepted by Addepar. Job ID = {addepar_job_id}")
    return addepar_job_id
    
def check_addepar_job_status(base_url, addepar_job_id, header) -> float:
    """
    Check the status of the specified Addepar API job. If the job is complete, download the data
    and save it.
    https://developers.addepar.com/docs/jobs#check-the-status-of-a-job
    
    **Once the job is complete, the response status code will change from 200 to 303.**
    The Addepar Jobs API status query will automatically redirect the request and download the
    job data once the job is complete. This can cuase issues as the status response is <1KB but
    the data payload can be >100KB+. To avoid this, allow_redirects=False in the API query. 

    Args:
        base_url (str): The base Addepar Jobs API URL
        addepar_job_id (int): The **Addepar** Jobs API ID of the job
        header (dict): Authorization header dictionary described in the Basic Authentication 
                section of Addepar's website

    Returns:
        float: The completion percentage of the job
    """
    
    # Query the status of the Addepar Job from the API
    url = f'{base_url}/{addepar_job_id}'
    try:
        r = requests.request('GET', url, headers=header, allow_redirects=False)
        logger.info(f"Addepar Job {addepar_job_id} status queried. HTTP response code = {r.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error encountered querying the status Addepar API job ID = {addepar_job_id}.")
        logger.error(e)
        return None
    
    # Decode the response
    json_str = r.content.decode('utf-8')
    
    # If the job status query was successful, Addepar will return a status code of 200
    # if the job is still in progress or 303 (redirection) if the job is complete.
    if not (r.status_code == 200 or r.status_code == 303):
        logger.error(f"Unexpected response status code from Addepar API Job status query.")
        
        # If the content is less than 800 bytes, print it to the log
        if len(r.content) <= 800:
            logger.info(json_str)
        else: 
            logger.info('Entire response is too large to display. Partial response:')
            logger.info(json_str[0:800])
            
        return None
    
    # Parse the response to JSON
    try:
        json_dict = json.loads(json_str)
    except json.JSONDecodeError as e:
        logger.error("Could not parse the decoded API response to JSON.")
        logger.error(e)
        return None 
    
    # Extract the Addepar API percent_complete attribute from the JSON dictionary
    try:
        addepar_pct_complete = json_dict['data']['attributes']['percent_complete']
        logger.info(f"Addepar Job {addepar_job_id} percent complete = {addepar_pct_complete} / 1.000")
    except KeyError as e:
        logger.error("Could not parse the percernt_complete attribute from the response string.")
        logger.error("Key Error: {e}")
        
        return None
    
    # If no errors were encountered, return the percent complete
    return addepar_pct_complete

def download_addepar_job(base_url, addepar_job_id, header, data_save_path) -> bool:
    """
    Download a completed Addepar API Job
    https://developers.addepar.com/docs/jobs#download-the-results-of-a-job

    Args:
        base_url (str): The base Addepar Jobs API URL
        addepar_job_id (int): The **Addepar** Jobs API ID of the job
        header (dict): Authorization header dictionary described in the Basic Authentication 
                section of Addepar's website
        data_save_path (str): full file path and name to save the returned API Job data

    Returns:
        bool: Success (True) or Failure (False) of downloading the job data
    """
    
    # Download the Addepar Job data from the API
    url = f'{base_url}/{addepar_job_id}/download'
    try:
        r = requests.request('GET', url, headers=header)
        logger.info(f"Addepar Job {addepar_job_id} job downloaded. HTTP response code = {r.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error encountered downloading the Addepar API Job ID = {addepar_job_id}.")
        logger.error(e)
        return False
    
    # Decode the response
    json_str = r.content.decode('utf-8')
    
    # If the job data download was successful
    if r.status_code != 200:
        logger.error(f"Unexpected response status code from Addepar API Job data download.")
        
        # If the content is less than 800 bytes, print it to the log
        if len(r.content) <= 800:
            logger.info(json_str)
        else: 
            logger.info('Entire response is too large to display. Partial response:')
            logger.info(json_str[0:800])
            
        return False
        
    # If the Addepar API Job data download was successful, save the data to the provided file path
    write_success = api_response_to_file(json_str, data_save_path)
    
    # If failed writing the JSON response to a file, return None indicating an error
    if not write_success:
        return False
    
    # If no errors were encountered, return True denoting the job data was successfully downloaded and saved
    return True
        
def update_job_status_db(conn, job_id, to_status, job_details) -> bool:
    """
    Update a specific job's status in the database JobQueue table using the 
    Addepar.usp_UpdateJobQueueStatus stored procedure. 
    *Job statuses should only be updated using this procedure to ensure the 
    audit data also gets updated.

    Args:
        conn (pyodbc.Connection): An open connection to a database server
        job_id (int): Unique database ID of the job to update the status of 
        to_status (str): Status name to update the 
        job_details (str): Job detials value to update the database with
                To Posted: The Addepar API job ID
                To Downloaded: The file path of the JSON data
                To Imported: The number of rows imported into the dbimport table
                To Completed: The number of rows imported into the target table

    Returns:
        bool: True if job status was successfully updated, False if error occurred
    """
    
    # Build the SQL statement
    sql = f"EXEC Addepar.usp_UpdateJobQueueStatus @JobQueueIdToUpdate={job_id}, "
    sql += f"@JobDetails='{job_details}', @UpdateToStatusName='{to_status}'"
    
    cursor = None
    try:
        # Execute the SQL
        logger.debug(sql)
        cursor = conn.cursor().execute(sql)
        
        # The proc returns a success or failure boolean
        update_success = cursor.fetchone()[0]
        
        # Since this proc updates a db table, need to commit it
        cursor.commit()
        
        # Clean up
        cursor.close()
    except pyodbc.Error as err:
        if cursor != None:
            cursor.close()
        logger.error(f"SQL error encountered. Job ID = {job_id} was not updated.")
        logger.error(err)
        return False
    
    if update_success == 1:
        # Status was successfully updated
        logger.info(f"Job ID = {job_id} status updated to '{to_status}' with details = {job_details}")
        return True
    else:
        # Failed to update job status
        logger.error(f"SQL error encountered. Job ID = {job_id} was not updated.")
        return False
    
def api_response_to_file(content, file_path) -> bool:
    """
    Write the content of an API response to a text file

    Args:
        content (str): Decoded API response to write to a file
        file_path (str): File path to write svae the response to

    Returns:
        bool: Success (True) or failure (False) of file write
    """
    
    cursor = None
    try:
        # Write to file
        with open(file_path, 'w') as outfile:
            outfile.write(content)
            
        logger.info(f"Wrote API reponse to file: {file_path}")
        return True
        
    except IOError as e:
        logger.error(f"IOError encountered: {e}")
        logger.error(f"Failed writing API response to file.")
        return False

def exec_import_proc(conn, sql) -> int:
    """
    Execute a post import proc string returned by SQL. Return the number of rows processed.

    Args:
        conn (pyodbc.Connection): An open connection to a database server
        sql (str): SQL EXEC string to execute

    Returns:
        int: Number of rows inserted. If an error was encountered, the method will return -1.
    """
    
    try:
        # Execute the SQL
        logger.debug(sql)
        cursor = conn.cursor().execute(sql)
        
        # The proc returns a success or failure boolean
        rows_inserted = cursor.fetchone()[0]
        
        # Since this proc updates a db table, need to commit it
        cursor.commit()
        
        # Clean up
        cursor.close()
    except pyodbc.Error as err:
        if cursor != None:
            cursor.close()
        logger.error(f"SQL error encountered executing proc. Data was not processed.")
        logger.error(err)
        return -1 

    logger.info(f"{rows_inserted} rows inserted.")
    return rows_inserted

if __name__ == "__main__":
    project_path = 'C:/Users/bstrathman/OneDrive - Lido Advisors, LLC/Documents/GitHub/AddeparRecon'
    log_path = f"{project_path}/logs"
    config = ConfigParser()
    config.read(f"{project_path}/config.ini")
    
    # Set up logging
    project_name = config.get('environment', 'project')
    log_timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    logging.config.fileConfig(
        f"{project_path}/logging_config.ini", 
        disable_existing_loggers=False, 
        defaults={"logfilename": f"{log_path}/{project_name}_{log_timestamp}.log"}
    )
    logger = logging.getLogger(__name__)
        
    # Get all the required parameters
    ###################################################################################################
    # Get database information
    SERVER = config.get('database', 'server')
    DATABASE = config.get('database', 'database')
    sql = "EXEC Addepar.usp_GetOpenJobs"
    
    # Get Addepar API information
    key = config.get('addepar_api', 'key')
    secret = config.get('addepar_api', 'secret')
    header = addepar_params.header 
    base_url = addepar_params.base_url
    auth_string = create_auth_string(key, secret)
    # Add the auth string to the API header
    header['Authorization'] = auth_string
    
    
    ###################################################################################################
    # Get data on all the open jobs from the database
    conn = dbutil.connect_to_database(SERVER, DATABASE)
    logger.info(f'Connected to {DATABASE} database on {SERVER} server.')
    open_jobs = dbutil.query_to_list(conn, sql)
    logger.info(f'{len(open_jobs)} open Addepar Jobs')
    
    # Loop through each open job
    """
    2023-01-18
    Currently no step is re-tried if it fails the first time. It is unknown how frequently posting or 
    getting data from the Addepar API will fail. If failures are frequent and it is found that adding
    a retry fixes most failures, automatic retries will be added later.
    """
    for job in open_jobs:
        # Unpack the SQL record dictionary returned by the Addepar.usp_GetOpenJobs proc
        job_id = job['ID']                      # Specific ID of the job in the Job Queue
        job_name = job['JobName']               # Name of the queued job ('Accounts' or 'Holdings')
        job_date = job['AsOfDate']              # Date as of which the job is being run
        job_status = job['StatusName']          # Current status of the job
        job_details = job['QueryParameters']    # Data required to complete the next step of the job
        """        
        If job status is 'Queued' job_details will be the required Addepar API job parameters
        If job status is 'Posted' job_detials will be the Addepar Job ID of the API query
        If job status is 'Downloaded' job_detials will be the full file path where the API data was saved
        If job status is 'Imported' job_detials will be the number of rows imported to into the DB 
        """
        
        # Direct traffic based on the current job status
        logger.info(f"Processing Job ID = {job_id}, job type = {job_name}, as of = {job_date}, status = {job_status}")
        match job_status:
            case 'Queued':
                # If the job is queued, the Addepar API job needs to be posted
                addepar_job_id = post_addepar_job(base_url, header, job_details, log_path)
                
                # If the post_addepar_job returns None, an error was encountered
                # Otherwise, it returns the integer Addepar API Job ID of the posted job
                if addepar_job_id is None:
                    logger.info("Updating Job ID = {job_id} status in database from 'Queued' to 'Error'")
                    update_job_status_db(conn, job_id, 'Error', 'Failure posting job to Addepar - see logs for details.')
                else:
                    logger.info("Updating job status in database from 'Queued' to 'Posted'")
                    update_job_status_db(conn, job_id, 'Posted', addepar_job_id)
            
            case 'Posted':
                # If the Addepar API job has been posted, check the status (and download if done)
                addepar_job_pct_complete = check_addepar_job_status(base_url, job_details, header)
                
                if addepar_job_pct_complete is None:
                    # Error was encountered checking the status of the job
                    logger.info(f"Updating Job ID = {job_id} status in database from 'Posted' to 'Error'")
                    update_job_status_db(conn, job_id, 'Error', 'Failure downloading API data from Addepar - see logs for details.')
                
                elif addepar_job_pct_complete >= 0 and addepar_job_pct_complete < 1:
                    # No error check job status but job is not yet complete
                    logger.info("Addepar API job not yet complete. Taking no further action.")
                    
                elif addepar_job_pct_complete == 1:
                    # Job was successfully completed download it now
                    logger.info("Addepar API job complete. Downloading it now.")
                    data_save_path = f'{project_path}/data/{job_name}_{job_date}.json'
                    download_success = download_addepar_job(base_url, job_details, header, data_save_path)
                    
                    if download_success:
                        logger.info("Updating job status in database from 'Posted' to 'Downloaded'")
                        update_job_status_db(conn, job_id, 'Downloaded', data_save_path)
                    else:
                        logger.error("Error importing job data into dbimport table of database.")
                        logger.info("Updating job status in database from 'Posted' to 'Error'")
                        update_job_status_db(conn, job_id, 'Error', 'Error import API data to datbase - see logs for details.')
                    
                else:
                    # Somehow got a percent_complete <0 or >1  -->  Throw an error
                    logger.error("ValueError: percent_complete = {addepar_job_pct_complete}. Should be between 0 and 1")
            
            case 'Downloaded':
                # If the data has been downloaded, import the data into the dbimport table
                rows_inserted = exec_import_proc(conn, job_details)
                
                # If an error was encountered, the method will return -1
                if rows_inserted == -1:
                    logger.error(f"Error importing {job_name} data into the dbimport table for Job ID = {job_id}.")
                    logger.info(f"Updating Job ID = {job_id} status in database from 'Downloaded' to 'Error'.")
                    update_job_status_db(conn, job_id, 'Error', 'Failure importing data into dbimport table - see logs for details.')
            
                if rows_inserted >= 0:
                    logger.info(f"{job_name} data imported into dbimport table for Job ID = {job_id}.")
                    logger.info(f"Updating Job ID = {job_id} status in database from 'Downloaded' to 'Imported'")
                    update_job_status_db(conn, job_id, 'Imported', rows_inserted)
            
            case 'Imported':
                # If the job has been imported to the dbimport table, run the post import proc
                rows_inserted = exec_import_proc(conn, job_details)
                
                # If an error was encountered, the method will return -1
                if rows_inserted == -1:
                    logger.error(f"Error processing {job_name} data into the target table for Job ID = {job_id}.")
                    logger.info(f"Updating Job ID = {job_id} status in database from 'Imported' to 'Error'.")
                    update_job_status_db(conn, job_id, 'Error', 'Failure importing data into target table - see logs for details.')
            
                if rows_inserted >= 0:
                    logger.info(f"{job_name} data processed into target table for Job ID = {job_id}.")
                    logger.info(f"Updating job status in database from 'Imported' to 'Completed'")
                    update_job_status_db(conn, job_id, 'Completed', rows_inserted) 
            
            case _:
                # Status is not matched - return an error
                logger.error(f"Unexpected job status returned. Please add code to handle the case where Job Status = {job_status}.")
     
    conn.close()
    logger.info("Database connection closed.")
    logger.info("Execution complete. Exiting program.")
    