import base64
import pickle

import googleapiclient.discovery
import prefect
from prefect import task
from prefect.engine import signals
import psycopg2
import os
import re
import glob
import requests
import geopandas as gp
from fiona import BytesCollection
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import configparser
from time import sleep
import subprocess as sp

from psycopg2.extras import LoggingConnection

#PREFECT__SERVER__HOST=https://k8s.alia-space.com/adm/prefect-api/?apikey=prefectAlia2022 prefect register -p DBTEST_DEV_workflow.py --project "NUTS-DEV" -l k8s

config_obj = configparser.ConfigParser()
config_obj.read("configfile.ini")

db_params = config_obj["postgresql"]
db_host = db_params["host"]
db_port = db_params["port"]
database = db_params["database"]
db_user = db_params["user"]
db_password = db_params["password"]

email_params = config_obj["notifier"]
email_sender = email_params["email_sender"]
email_credentials = email_params["credentials"]

geoserver_params = config_obj["geoserver"]
geoserver_ows = geoserver_params["ows_host"]

fold_params = config_obj["res_folders"]
cache_folder = fold_params["cache_folder"]
result_folder = fold_params["result_folder"]
results_url = fold_params["results_url"]
results_user = fold_params["res_user"]
results_psw = fold_params["res_psw"]

@task
def updateTimeStamp(orderid, column):
    # logging connection
    logger = prefect.context.get("logger")

    # establish connection with db
    conn = psycopg2.connect(
        host=db_host,
        database=database,
        user=db_user,
        password=db_password,
        port=db_port
    )
    cursor = conn.cursor()
    insert_into_order = "UPDATE order_tracking SET " + column + " = NOW() WHERE order_id = " + str(orderid)
    cursor.execute(insert_into_order)
    conn.commit()
    conn.close()
    logger.info("Timestamp " + str(column) + " updated  for order " + str(orderid))



@task
def retrieveShape(orderid):
    # logging connection
    logger = prefect.context.get("logger")

    # establish connection with db
    conn = psycopg2.connect(
        host=db_host,
        database=database,
        user=db_user,
        password=db_password,
        port=db_port
    )
    cursor = conn.cursor()

    # read inputs from order_tracking
    order_request = "SELECT shape_layer, shape_roi FROM order_tracking WHERE order_id = '" + str(orderid) + "'"
    cursor.execute(order_request)
    order = cursor.fetchone()
    shape_layer = order[0]
    shape_roi = order[1]

    conn.close()

    if 'NUTS' in shape_layer:
        level = shape_layer.split('_')[1]
        layer = f'nuts:NUTS_RG_01M_2021_3857_LEVL_{level}'
        feature = shape_roi
        cql = 'NAME_LATN'
        crs = 'epsg:3857'
    elif 'GADM' in shape_layer:
        level = shape_layer.split('_')[1]
        layer = f'gadm:gadm36_{level}'
        feature = shape_roi
        cql = f'NAME_{level}'
        crs = 'epsg:3857'

    logger.info("Retrieving shapefile for " + str(shape_roi))

    try:
        def wfs2gp_df(url, layer_name, feature_id, cql_name, wfs_version="2.0.0"):
            req_url = f"{url}?service=wfs&version={wfs_version}&request=GetFeature&typeNames={layer_name}&CQL_FILTER={cql_name}='{feature_id}'"
            logger.info("Request url: " + str(req_url))
            with BytesCollection(requests.get(
                    f"{url}?service=wfs&version={wfs_version}&request=GetFeature&typeNames={layer_name}&CQL_FILTER={cql_name}='{feature_id}'"
            ).content) as f:
                df = gp.GeoDataFrame.from_features(f, crs=crs)
            return df

        geo_data = wfs2gp_df(geoserver_ows, layer, feature, cql_name=cql)
        geo_data.crs = crs

        # save shapefile to disk (in shapefiles folder)
        shapefile_dir = f'{result_folder}/shapefiles'
        if not os.path.exists(shapefile_dir):
            os.makedirs(shapefile_dir)
        shpid = re.sub(r'[^\w]', '', shape_roi)
        shapefile_path = f'{shapefile_dir}/shape_{shpid}.shp'
        geo_data.to_file(shapefile_path)
        geo_data_4326 = geo_data.to_crs(epsg=4326)
        geo_data_boundary = geo_data_4326.total_bounds
        logger.info("Shapefile obtained!")
        geo_data_32633 = geo_data.to_crs(epsg=32633)
        shape_area = geo_data_32633['geometry'].area.values[0] / (10**6)
    except ValueError as e:
        logger.info("Shapefile NOT obtained!")
        logger.info(str(e))
        geo_data_boundary = []
        shape_area = 0
    finally:
        geo_data_info = [geo_data_boundary, shape_area]

    return geo_data_info

@task
def organizeJobs(downloaded_products, orderid):

    # establish connection with db
    conn = psycopg2.connect(
        host=db_host,
        database=database,
        user=db_user,
        password=db_password,
        port=db_port
    )
    cursor = conn.cursor()

    # logging connection
    logger = prefect.context.get("logger")

    logger.info("Working on " + str(downloaded_products))

    # retrieve jobs from db based on order-id
    job_request = "SELECT job_id FROM job WHERE order_id = '" + str(orderid) + "'"
    cursor.execute(job_request)
    jobs = cursor.fetchall()
    logger.info("The products belong to job = " + str(jobs))

    conn.close()

    job_list = []
    for job in jobs:
        job_list.append(job[0])

    return job_list


@task
def notifyResult(jobid, orderid):

    # logging connection
    logger = prefect.context.get("logger")
    logger.info(" Currently on job: " + str(jobid))

    # establish connection with db
    conn = psycopg2.connect(
        host=db_host,
        database=database,
        user=db_user,
        password=db_password,
        port=db_port
    )
    cursor = conn.cursor()

    # retrieve the userid from db to get email address
    request = "SELECT uuid_user FROM order_tracking WHERE order_id = '" + str(orderid) + "'"
    cursor.execute(request)
    req = cursor.fetchone()
    userid = req[0]

    # retrieve the email address from db to notify user
    request = "SELECT * FROM public.user WHERE name = '" + str(userid) + "'"
    cursor.execute(request)
    req = cursor.fetchone()
    username = req[1]
    receiver_email = req[2]

    # move the tif files from job_x to order_x folder and remove the former
    logger.info("Moving final products in order folder ...")
    output_folder = os.path.join(result_folder, f'products_ready/{username}/order_{orderid}/')
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    for j in jobid:
        job_folder = f'{result_folder}/working_dir/job_{j}'
        list_images_tif = glob.glob(f"{job_folder}/*.tif")
        for tif in list_images_tif:
            cmd = f'mv {tif} {output_folder}/'
            os.system(cmd)
            logger.info("File " + str(tif) + " moved in folder " + str(output_folder))
        cmd = f'rm -r {job_folder}'
        os.system(cmd)

    message = MIMEMultipart("alternative")
    message["Subject"] = "GDS products"
    message["From"] = email_sender
    message["To"] = receiver_email

    # Create the plain-text and HTML version of your message
    if not os.listdir(output_folder):
        text = f"""\
        Hi,
        There are no product available for your request.
        
        Help yourself performing an estimation first.
    
        Best regards,
        NUTS team
        """
        html = f"""\
        <html>
          <body>
            <p>Hi,<br>
               There are no product available for your request.<br>
               Help yourself performing an estimation first. 
               <br>
               <br>
               Best regards, 
               NUTS team
            </p>
          </body>
        </html>
        """
    else:
        #user_res = f'{results_url}/{username}/order_{orderid}/'
        user_res = f'{results_url}/order_{orderid}/'
        text = f"""\
        Hi,
        All the products you requested are ready for you!

        Go to {user_res} to download the products.

        Best regards,
        NUTS team
        """
        html = f"""\
        <html>
          <body>
            <p>Hi,<br>
               All the products you requested are ready for you!<br>
               Go to 
               <a href={user_res}>your results section</a> to download the products.
               <br>
               <br>
               Best regards, 
               NUTS team
            </p>
          </body>
        </html>
        """
    # Turn these into plain/html MIMEText objects
    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")

    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part1)
    message.attach(part2)

    # Create a secure SSL context
    context = ssl.create_default_context()

    # TODO: consider adding a condition to determine if job is completed
    # if job_status == 'completed':

    pickle_path = os.path.join(os.getcwd(), email_credentials)
    # Load our pickled credentials
    creds = pickle.load(open(pickle_path, 'rb'))
    # Build the service
    service = googleapiclient.discovery.build('gmail', 'v1', credentials=creds)
    raw = base64.urlsafe_b64encode(message.as_bytes())
    raw = raw.decode()
    body = {'raw': raw}
    message = (
        service.users().messages().send(
            userId="me", body=body).execute())
    logger.info('Message sent with Id: %s' % message['id'])

    insert_order_status = "UPDATE order_tracking SET status = %s, download_url = %s WHERE order_id = %s"
    cursor.execute(insert_order_status, ("user notified", user_res, orderid))
    conn.commit()
    conn.close()
    logger.info("Database updated with order status.")
    # else:
    #     insert_order_status = "UPDATE order_tracking SET status = %s WHERE order_id = %s"
    #     cursor.execute(insert_order_status, ("failed", orderid))
    #     conn.commit()
    #     logger.info("Database updated with order status (failed).")


@task
def toggleStatus(orderid):
    # logging connection
    logger = prefect.context.get("logger")

    i = 0
    while True:
        # establish connection with db
        conn = psycopg2.connect(
            host=db_host,
            database=database,
            user=db_user,
            password=db_password,
            port=db_port
        )
        cursor = conn.cursor()
        insert_order_status = "UPDATE order_tracking SET status = %s WHERE order_id = %s"
        cursor.execute(insert_order_status, ("test-" + str(i), orderid))
        logger.info("Database updated with order test status:" + str(i))
        conn.commit()
        i += 1
        sleep(1)
        prefect.client.client.Client.update_flow_run_heartbeat

        conn.close()

@task
def cleanup(orderid):

    # logging connection
    logger = prefect.context.get("logger")
    logger.info(" Cleanup on order: " + str(orderid))

    # establish connection with db
    conn = psycopg2.connect(
        connection_factory=LoggingConnection,
        host=db_host,
        database=database,
        user=db_user,
        password=db_password,
        port=db_port
    )

    conn.initialize(logger)

    cursor = conn.cursor()



    receiver_email = None

    try:
        # retrieve the userid from db to get email address
        logger.info("Retrieve user ID for order " + str(orderid))
        request = "SELECT uuid_user FROM order_tracking WHERE order_id = '" + str(orderid) + "'"
        cursor.execute(request)
        req = cursor.fetchone()
        userid = req[0]

        logger.info("The user ID for order " + str(orderid) + " is " + str(userid))

        # retrieve the email address from db to notify user
        logger.info("Retrieve user detail for ID " + str(userid))
        request = "SELECT * FROM public.user WHERE name = '" + str(userid) + "'"
        cursor.execute(request)
        req = cursor.fetchone()
        username = req[1]
        receiver_email = req[2]
        logger.info("The user name for ID " + str(orderid) + " is " + str(username))

        # retrieve jobs from db based on order-id
        logger.info("Retrieve jobs for order " + str(orderid))
        job_request = "SELECT job_id FROM job WHERE order_id = '" + str(orderid) + "'"
        cursor.execute(job_request)
        jobs = cursor.fetchall()

        logger.info("The folder to cleanup belong to job = " + str(jobs))

        if jobs != None:
            jobid = []
            for job in jobs:
                jobid.append(job[0])

            for j in jobid:
                job_folder = f'{result_folder}/working_dir/job_{j}'
                try:
                    cmd = f'rm -r {job_folder}'
                    os.system(cmd)
                except Exception as e:
                    logger.info(" Exception removing dir " + str(job_folder) + " : " + str(e))
    except Exception as e:
        logger.info(" Exception getting order detail " + str(orderid) + " : " + str(e))

    if receiver_email != None:

        message = MIMEMultipart("alternative")
        message["Subject"] = "GDS products"
        message["From"] = email_sender
        message["To"] = receiver_email

        text = f"""\
            Hi,
            Some error occurred while processing your request.
            
            Help yourself performing an estimation first.
        
            Best regards,
            NUTS team
            """
        # Turn these into plain/html MIMEText objects
        part1 = MIMEText(text, "plain")

        # Add HTML/plain-text parts to MIMEMultipart message
        # The email client will try to render the last part first
        message.attach(part1)

        # TODO: consider adding a condition to determine if job is completed
        # if job_status == 'completed':
        pickle_path = os.path.join(os.getcwd(), email_credentials)
        # Load our pickled credentials
        creds = pickle.load(open(pickle_path, 'rb'))
        # Build the service
        service = googleapiclient.discovery.build('gmail', 'v1', credentials=creds)
        raw = base64.urlsafe_b64encode(message.as_bytes())
        raw = raw.decode()
        body = {'raw': raw}
        message = (
            service.users().messages().send(
                userId="me", body=body).execute())
        logger.info('Message sent with Id: %s' % message['id'])
    try:
        insert_order_status = "UPDATE order_tracking SET status = %s WHERE order_id = %s"
        cursor.execute(insert_order_status, ("ERROR", orderid))
        conn.commit()
        logger.info("Database updated with order status (error).")
    except Exception as e:
        logger.info(" Exception updating order status " + str(orderid) + " : " + str(e))
    conn.close()
    raise signals.FAIL()

@task
def log_message(msg):
    logger = prefect.context.get("logger")
    logger.info(str(msg))
