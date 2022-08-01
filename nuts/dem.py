import re
import os
from osgeo import gdal
# from owslib.wms import WebMapService
import requests
import prefect
from prefect import task
import psycopg2
import psycopg2.extras
import numpy as np
import datetime
import configparser

config_obj = configparser.ConfigParser()
config_obj.read("configfile.ini")

db_params = config_obj["postgresql"]
db_host = db_params["host"]
db_port = db_params["port"]
database = db_params["database"]
db_user = db_params["user"]
db_password = db_params["password"]

geoserver_params = config_obj["geoserver"]
geoserver_url = geoserver_params["wms_host"]
geoserver_wcs = geoserver_params["wcs_host"]

fold_params = config_obj["res_folders"]
cache_folder = fold_params["cache_folder"]
res_folder = fold_params["result_folder"]


@task
def downloadDEMproduct(orderid, shapeBoundaries):
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

    shapeBounds = shapeBoundaries[0]

    # read inputs from order_tracking
    order_request = "SELECT * FROM order_tracking WHERE order_id = '" + str(orderid) + "'"
    cursor.execute(order_request)
    order = cursor.fetchone()
    #order = dict(zip(zip(*cursor.description)[0], cursor.fetchone()))
    shape_roi = order["shape_roi"]
    shpid = re.sub(r'[^\w]', '', shape_roi)
    #data_type = order[5]

    sensing_start = order["start_time"]
    sensing_stop = order["stop_time"]

    # create list of years
    year_range = [year for year in range(sensing_start.year, sensing_stop.year + 1)]
    product_list = []

    # iterate over years
    cursor = conn.cursor()
    for year in year_range:
        layer_name = f'gds:Africa_DEM_{year}_DS'
        output_name_def = f'{cache_folder}/Africa_DEM_{year}_DS_{shpid}.tif'

        logger.info('Retrieving ' + str(layer_name))

        # update db with new order and retrieve job id
        start_date = datetime.date(year, 1, 1)
        stop_date = datetime.date(year, 12, 31)
        insert_jobs = "INSERT INTO job (order_id, start_date, end_date) VALUES (%s, %s, %s) RETURNING job_id"  # 888
        records_to_insert = (orderid, start_date, stop_date)
        cursor.execute(insert_jobs, records_to_insert)
        conn.commit()
        jobid = cursor.fetchone()[0]
        # create the job dir
        job_folder = f'{res_folder}/working_dir/job_{jobid}'
        if not os.path.exists(job_folder):
            os.makedirs(job_folder)
        output_name = f'{job_folder}/Africa_DEM_{year}_DS_{shpid}_tmp.tif'

        layer_name = layer_name.replace(":", "__")
        # download product if not already available in cache_folder
        if os.path.isfile(output_name_def):
            logger.info("Product already downloaded!")
            download_status = 'downloaded'
        else:
            try:
                url = f"{geoserver_wcs}?service=WCS&version=2.0.1&request=getcoverage" \
                      "&coverageid=" + layer_name + "&subset=Long(" + str(shapeBounds[0]) + "," + str(shapeBounds[2]) + ")" \
                      "&subset=Lat(" + str(shapeBounds[1]) + "," + str(shapeBounds[3]) + ")"
                r = requests.get(url, allow_redirects=True)
                with open(output_name, 'wb') as out:
                    out.write(r.content)
                cmd_move = f'mv {output_name} {output_name_def}'
                os.system(cmd_move)
                logger.info('Output available as ' + str(output_name_def))
                download_status = 'downloaded'
            except ConnectionError:
                download_status = 'failed'
                logger.info('Required product not available.')


        # update db with products and retrieve product id
        insert_product = "INSERT INTO source_product (job_id, uri, product_name, status) VALUES (%s, %s, %s, %s) RETURNING source_product_id"
        product_to_insert = (jobid, layer_name, output_name, download_status)
        cursor.execute(insert_product, product_to_insert)
        conn.commit()
        prod_index = cursor.fetchone()[0]
        product_list.append(prod_index)
        conn.close()

        return product_list


@task
def cropDEMproducts(jobid):
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

    # retrieve the orderid and year from db
    orderid_request = "SELECT * FROM job WHERE job_id = '" + str(jobid) + "'"
    cursor.execute(orderid_request)
    order = cursor.fetchone()
    orderid = order[1]
    year = order[2].year

    # read inputs from order_tracking
    order_request = "SELECT shape_roi FROM order_tracking WHERE order_id = '" + str(orderid) + "'"
    cursor.execute(order_request)
    order = cursor.fetchone()
    shape_roi = order[0]

    # point the shapefile
    shpid = re.sub(r'[^\w]', '', shape_roi)
    shp_file = f'{res_folder}/shapefiles/shape_{shpid}.shp'
    logger.info("The chosen ROI is " + str(shp_file))

    # define the job dir path
    job_folder = f'{res_folder}/working_dir/job_{jobid}'

    # select input and output file based on product type
    output_file = f'{job_folder}/Africa_DEM_{year}_DS_{shpid}.tif'
    output_file_tmp = f'{job_folder}/Africa_DEM_{year}_DS_{shpid}_tmp.tif'
    original_dem = f'{cache_folder}/Africa_DEM_{year}_DS_{shpid}.tif'

    # crop input data as for shapefile
    try:
        ds = gdal.Open(original_dem, 1)
        gdal.Warp(output_file_tmp, ds, format='GTiff', cutlineDSName=shp_file, cutlineLayer=f'shape_{shpid}', cropToCutline=True, dstNodata=np.nan)
        topts = gdal.TranslateOptions(creationOptions=['COMPRESS=LZW', 'PREDICTOR=2'])
        gdal.Translate(output_file, output_file_tmp, options=topts)
        rm_tmp = f'rm {output_file_tmp}'
        os.system(rm_tmp)
        ds = None
    except SystemError:
        logger.info('No products to be cropped. Control if original data is available.')

    # update db with job status
    if not os.path.exists(output_file):
        status = 'failed'
    else:
        status = 'completed'

    insert_job_status = "UPDATE job SET status = %s WHERE job_id = %s"
    cursor.execute(insert_job_status, (status, jobid))
    conn.commit()
    conn.close()
    logger.info("Database updated with job status.")

    return jobid

