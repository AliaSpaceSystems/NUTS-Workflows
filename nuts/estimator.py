import base64
import os
import pickle

from prefect import task
import prefect

import requests
import io
import pandas as pd
import geopandas as gp
import psycopg2
import psycopg2.extras
import datetime

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import googleapiclient.discovery

import configparser

from nuts.demgeojson import demgeojson

config_obj = configparser.ConfigParser()
config_obj.read("configfile.ini")

db_params = config_obj["postgresql"]
db_host = db_params["host"]
db_port = db_params["port"]
database = db_params["database"]
db_user = db_params["user"]
db_password = db_params["password"]

s2_hub = config_obj["s2hub"]
tables = s2_hub['csv_tables']
username = s2_hub['user']
password = s2_hub['password']

email_params = config_obj["notifier"]
email_sender = email_params["email_sender"]
email_credentials = email_params["credentials"]

run_settings = config_obj["run"]
server_req = run_settings["server_add"]

fold_params = config_obj["res_folders"]
res_folder = fold_params["result_folder"]


@task
def s2estimator(orderid, source_products):
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

    bytes_to_GB = 9.31 * 10e-11
    bytes_to_MB = 9.537 * 10e-08

    final_size = 0
    online_prods = 0
    offline_prods = 0
    no_response = 0
    job_list = []

    # find the snapeshot date to access csv files
    url_dir = f'{tables}/S2A/2015/07'
    folder = requests.get(url_dir, auth=(username, password))
    content = folder.text
    snapshot_date = content.split('OPENHUB_catalogue_')[1]
    snapshot_date = snapshot_date.split('.csv')[0]

    for item in source_products:
        # get the source product from source product ids
        order_request = "SELECT * FROM source_product WHERE source_product_id = '" + str(item) + "'"
        cursor.execute(order_request)
        product = cursor.fetchone()
        jobid = product[1]
        job_folder = f'{res_folder}/working_dir/job_{jobid}'
        cmd_remove_job_fold = f'rm -r {job_folder}'
        os.system(cmd_remove_job_fold)
        logger.info('Working on job = ' + str(jobid) + ' and product = ' + str(item))
        if jobid not in job_list:
            job_list.append(jobid)
        uri = product[2]
        prod_name = product[3]
        logger.info("Checking for product: " + str(prod_name))
        url = f"{uri}/Online/$value"
        try:
            online_product = requests.get(url, auth=(username, password))
            logger.info("The product is online: " + str(online_product.text))
            if 'true' in online_product.text:
                prod_status = 'online (estimate)'
                online_prods += 1
            elif 'false' in online_product.text:
                prod_status = 'offline (estimate)'
                offline_prods += 1
        except requests.exceptions.RequestException as e:
            logger.info("No info available :" + str(e))
            prod_status = 'undetermined (estimate)'
            no_response += 1

        logger.info(str(prod_name) + " is " + str(prod_status))

        satellite = prod_name.split('_')[0]
        date = prod_name.split('_')[2]
        date_for_url = date.split('T')[0]
        year = date[:4]
        month = date[4:6]

        file = f'{satellite}_{date_for_url}_OPENHUB_catalogue_{snapshot_date}.csv'
        url_csv = f'{tables}/{satellite}/{year}/{month}/{file}'
        logger.info('Looking for product size at ' + str(url_csv))
        cur_size = 0
        try:
            get_csv = requests.get(url_csv).content
            df = pd.read_csv(io.StringIO(get_csv.decode('utf-8')))
            # df = pd.read_csv(url_csv)
            prod_size = df[['ContentLength']][df['Name'] == prod_name]
            cur_size = prod_size.values[0][0]
            logger.info('Product size: ' + str(cur_size))
        except pd.errors.ParserError:
            cur_size = 500000000
            logger.info('Size not available, assuming ' + str(cur_size * bytes_to_GB))
        finally:
            # update total size
            final_size += cur_size
            # update db with download status
            insert_status = "UPDATE source_product SET status = %s WHERE source_product_id = %s"
            cursor.execute(insert_status, (prod_status, item))
            conn.commit()

    final_size_GB = "{:.2f}".format(final_size * bytes_to_GB)

    logger.info('Found ' + str(len(source_products)) + ' products on the catalogue.')
    logger.info('Online products = ' + str(online_prods) + ' - Offline products = ' + str(offline_prods))
    if no_response > 0:
        logger.info('Connection error with scihub ... \n Undetermined products: ' + str(no_response))
    logger.info('Total size of original files = ' + str(final_size_GB) + ' GB')

    # update db with order status
    insert_status = "UPDATE order_tracking SET status = %s, weight = %s WHERE order_id = %s"
    cursor.execute(insert_status, ('estimated', str(int(final_size*bytes_to_MB*1024)), orderid))
    conn.commit()
    conn.close()
    estimation = [len(source_products), final_size_GB, job_list]

    return estimation


@task
def demestimator(orderid, shapeBoundaries):
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
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    shape_area = shapeBoundaries[1]

    order_request = "SELECT * FROM order_tracking WHERE order_id = '" + str(orderid) + "'"
    cursor.execute(order_request)
    order = cursor.fetchone()
    #order = dict(zip(zip(*cursor.description)[0], cursor.fetchone()))
    sensing_start = order["start_time"]
    sensing_stop = order["stop_time"]

    shpid = order["shape_roi"]
    shapefile_dir = f'{res_folder}/shapefiles'
    shape_path = f'{shapefile_dir}/shape_{shpid}.shp'
    shape = gp.read_file(shape_path)
    shape_geo = shape[['geometry']].to_crs(epsg=4326)
    area_shape = shape_geo.area
    logger.info("Required ROI = " + str(shpid))

    cover_data = gp.read_file(demgeojson, driver='GeoJSON')

    cover_data_4326 = cover_data.to_crs(epsg=4326)

    intersect_prod = gp.overlay(shape_geo, cover_data_4326, how='intersection')
    if not intersect_prod.empty:
        coverage_perc = round((intersect_prod.unary_union.area/shape_geo.unary_union.area * 100.0), 2)
    else:
        coverage_perc = 0.0

    # create list of years
    year_range = [year for year in range(sensing_start.year, sensing_stop.year + 1)]
    number_of_files = len(year_range)

    if coverage_perc < 1.0:
        number_of_files = 0

    unit_size_GB = 1
    size_to_KB = 2222
    logger.info("Shape area = " + str(shape_area) + " with coverage " + str(coverage_perc) + "%")

    final_size = number_of_files*shape_area*unit_size_GB

    estimation = [number_of_files, final_size, coverage_perc]
    logger.info('Found ' + str(number_of_files) + ' product(s). Total area = ' + str(final_size) + ' km^2 whith ' + str(coverage_perc) + "% coverage")

    # update db with order status
    insert_status = "UPDATE order_tracking SET status = %s, weight = %s WHERE order_id = %s"
    cursor.execute(insert_status, ('estimated', str(int(final_size*size_to_KB)), orderid))
    conn.commit()
    conn.close()
    return estimation



@task
def notifyS2EstResult(orderid, estimation):

    prods_number = estimation[0]
    final_size_GB = estimation[1]
    job_list = estimation[2]

    # logging connection
    logger = prefect.context.get("logger")
    logger.info(" Currently on order: " + str(orderid))

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
    logger.info(str(req))
    userid = req[0]

    # retrieve the email address from db to notify user
    request = "SELECT * FROM public.user WHERE name = '" + str(userid) + "'"
    cursor.execute(request)
    req = cursor.fetchone()
    username = req[1]
    receiver_email = req[2]

    details = []

    for jobid in job_list:

        online_prods = 0
        unavailable_prods = 0

        # retrieve the date from db
        date_request = "SELECT * FROM job WHERE job_id = '" + str(jobid) + "'"
        cursor.execute(date_request)
        date = cursor.fetchone()[2]
        date_format = datetime.datetime.strftime(date, '%Y-%m-%d')

        prods_request = "SELECT * FROM source_product WHERE job_id = '" + str(jobid) + "'"
        cursor.execute(prods_request)
        prods = cursor.fetchall()
        for item in prods:
            if 'online' in item[-1]:
                online_prods += 1
            elif 'undetermined' in item[-1]:
                unavailable_prods += 1

        if unavailable_prods > 0 and online_prods == 0:
            details.append(
                f'For the date {date_format} there are {len(prods)} products, unfortunately their status is unavailable.')

        details.append(
            f'For the date {date_format} there are {len(prods)} products, where {online_prods} are online and {len(prods) - online_prods} are offline')

    message = MIMEMultipart("alternative")
    message["Subject"] = "GDS estimate"
    message["From"] = email_sender
    message["To"] = receiver_email

    # Create the plain-text and HTML version of your message

    list_det = (["<li>{}</li>".format(x) for x in details])
    logger.info(str(list_det))

    if list_det:

        text = f"""\
        Hi,
        We have estimated your order!
        For your request, {prods_number} products are available.
        The total size of native products is {final_size_GB} Gb.
        """ + ''.join(list_det) + f"""
        If you are satisfied, confirm the execution of your request by clicking
        {server_req}/nuts/executeOrder/{orderid}
    
        Best regards,
        NUTS team"""

        html = f"""\
        <html>
          <body>
            <p>Hi, <br>
               <br>
               We have estimated your order!<br>
               For your request, {prods_number} products are available. 
               The total size of native products is {final_size_GB} Gb.<br> 
               """ + ''.join(list_det) + f"""
               <br> If you are satisfied, confirm the execution of your request by clicking: <a href='{server_req}/nuts/executeOrderById/{orderid}'>NUTS executor.</a>
               <br>
               <br>
               Best regards, <br>
               NUTS team <br>
            </p>
          </body>
        </html>
        """

    else:
        text = f"""\
        Hi,
        We have estimated your order!
        There are no products available for your request.
        Try to perform a different request.
        
        Best regards,
        NUTS team"""

        html = f"""\
        <html>
          <body>
            <p>Hi, <br>
               <br>
               We have estimated your order!<br>
               There are no products available for your request. 
               Try to perform a different request.<br> 
               <br>
               <br>
               Best regards, <br>
               NUTS team <br>
            </p>
          </body>
        </html>
        """


    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")

    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part1)
    message.attach(part2)

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

    insert_order_status = "UPDATE order_tracking SET status = %s WHERE order_id = %s"
    cursor.execute(insert_order_status, ("user notified (estimate)", orderid))
    conn.commit()
    conn.close()
    logger.info("Database updated with order status.")

@task
def notifyDemEstResult(orderid, estimation):

    prods_number = estimation[0]
    final_size_GB = estimation[1]
    coverage_perc = estimation[2]

    # logging connection
    logger = prefect.context.get("logger")
    logger.info(" Currently on order: " + str(orderid))

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
    logger.info(str(req))
    userid = req[0]

    # retrieve the email address from db to notify user
    request = "SELECT * FROM public.user WHERE name = '" + str(userid) + "'"
    cursor.execute(request)
    req = cursor.fetchone()
    username = req[1]
    receiver_email = req[2]

    message = MIMEMultipart("alternative")
    message["Subject"] = "GDS estimate"
    message["From"] = email_sender
    message["To"] = receiver_email

    if prods_number != 0:

        # Create the plain-text and HTML version of your message
        text = f"""\
            Hi,
            We have estimated your order!
            For your request, {prods_number} products are available.
            The area covered by your request is {final_size_GB} km2 with {coverage_perc}% of data coverage.
            If you are satisfied, confirm the execution of your request by clicking
            {server_req}/nuts/executeOrderById/{orderid}
            Best regards,
            NUTS team"""
        html = f"""\
        <html>
          <body>
            <p>Hi, <br>
               <br>
               We have estimated your order!<br>
               For your request, {prods_number} products are available. <br>
               The area covered by your request is {final_size_GB} km2 with {coverage_perc}% of data coverage. <br>
               <br>
               If you are satisfied, confirm the execution of your request by clicking: <a href='{server_req}/nuts/executeOrder/{orderid}'>NUTS executor.</a>
               <br>
               <br>
               Best regards, <br>
               NUTS team <br>
            </p>
          </body>
        </html>
        """
    else:
        # Create the plain-text and HTML version of your message
        text = f"""\
        Hi,
        We have estimated your order!
        There are no products available for your request.
        Try to perform a different request on the Africa continent.

        Best regards,
        NUTS team"""

        html = f"""\
        <html>
          <body>
            <p>Hi, <br>
               <br>
               We have estimated your order!<br>
               There are no products available for your request. 
               Try to perform a different request on the Africa continent.<br> 
               <br>
               <br>
               Best regards, <br>
               NUTS team <br>
            </p>
          </body>
        </html>
        """

    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")

    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part1)
    message.attach(part2)

    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")

    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part1)
    message.attach(part2)

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

    insert_order_status = "UPDATE order_tracking SET status = %s WHERE order_id = %s"
    cursor.execute(insert_order_status, ("user notified (estimate)", orderid))
    conn.commit()
    conn.close()
    logger.info("Database updated with order status.")
