from prefect import task
import os
import zipfile
from osgeo import gdal
import prefect
import psycopg2
import psycopg2.extras
import glob
from pathlib import Path
import numpy as np
import re
from datetime import datetime
import configparser
import fnmatch
from math import floor

config_obj = configparser.ConfigParser()
config_obj.read("configfile.ini")

db_params = config_obj["postgresql"]
db_host = db_params["host"]
db_port = db_params["port"]
database = db_params["database"]
db_user = db_params["user"]
db_password = db_params["password"]

fold_params = config_obj["res_folders"]
cache_fold = fold_params["cache_folder"]
res_folder = fold_params["result_folder"]


@task
def mergeProducts(jobid):

    # logging connection
    logger = prefect.context.get("logger")
    logger.info(" Currently on job: " + str(jobid))

    def progress_callback(complete, message, unknown):
        percent = complete * 100.0
        logger.info('progress: {:.2f}%, message: "{}", unknown {}'.format(percent, message, unknown))
        return 1


    # establish connection with db
    conn = psycopg2.connect(
        host=db_host,
        database=database,
        user=db_user,
        password=db_password,
        port=db_port
    )
    cursor = conn.cursor()

    # get the products name from jobid
    order_request = "SELECT product_name FROM source_product WHERE job_id = '" + str(jobid) + "'"
    cursor.execute(order_request)
    product_list = cursor.fetchall()
    product_name_list = []
    for i in product_list:
        product_name_list.append(i[0])

    logger.info("Retrieved products name from  source_product: " + str(product_name_list))

    # retrieve the orderid from db
    orderid_request = "SELECT order_id FROM job WHERE job_id = '" + str(jobid) + "'"
    cursor.execute(orderid_request)
    orderid = cursor.fetchone()[0]

    logger.info("Retrieved orderID from  job_id: " + str(orderid))

    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    # retrieve the shapefile path and the mission from db
    request = "SELECT * FROM order_tracking WHERE order_id = '" + str(orderid) + "'"
    cursor.execute(request)
    req = cursor.fetchone()
    #req = dict(zip(zip(*cursor.description)[0], cursor.fetchone()))
    shp = req["shape_roi"]
    datatype = req["source_data_type"]
    cursor = conn.cursor()

    logger.info("Retrieved shape_layer from  order_tracking: " + str(shp))

    # redefine the shapefile path for the gdal command
    shpid = re.sub(r'[^\w]', '', shp)
    shp_file = f'{res_folder}/shapefiles/shape_{shpid}.shp'
    logger.info("The chosen ROI is " + str(shp_file))

    logger.info("Merging the following products" + str(product_name_list))

    products_folder = cache_fold

    # create job folder and output folder
    job_folder = f'{res_folder}/working_dir/job_{jobid}'

    for product_name in product_name_list:
        logger.info('Reading zip file list: ' + str(product_name))
        zip_filename = f'{cache_fold}/{product_name}.zip'
        zip = zipfile.ZipFile(zip_filename)
        list_zip = zip.namelist()
        if 'S2MSI1C' in datatype:
            #list_images = glob.glob(f"{job_folder}/*.SAFE/GRANULE/*/IMG_DATA/*{band}.jp2")
            list_result = fnmatch.filter(list_zip, "*.SAFE/GRANULE/*/IMG_DATA/*.jp2")
        elif 'S2MSI2A' in datatype:
            #list_images = glob.glob(f"{job_folder}/*.SAFE/GRANULE/*/IMG_DATA/*/*{band}.jp2")
            list_result = fnmatch.filter(list_zip, "*.SAFE/GRANULE/*/IMG_DATA/*/*.jp2")

        for i in list_result:
            tmp_out = i.split('.jp2')
            tmp_out = f"{job_folder}/{tmp_out[0]}.vrt"
            try:
                os.makedirs(os.path.dirname(tmp_out))
            except Exception as e:
                logger.info('Exception creating directory ' + str(os.path.dirname(tmp_out)) + ": " + str(e))
            logger.info('Generating vrt: ' + str(tmp_out))
            ds = gdal.Open(f"/vsizip/{zip_filename}/{i}")
            gdal.Warp(tmp_out, ds, dstSRS='epsg:4326', dstNodata=np.nan)



    # define the list of bands to work on Sentinel-2 data
    if 'S2MSI1C' in datatype:
        bands_list = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B09', 'B10', 'B11', 'B12', 'TCI']
    elif 'S2MSI2A' in datatype:
        bands_list = ['AOT_10m', 'B02_10m', 'B03_10m', 'B04_10m', 'B08_10m', 'TCI_10m', 'WVP_10m',
                      'AOT_20m', 'B01_20m', 'B02_20m', 'B03_20m', 'B04_20m', 'B05_20m', 'B06_20m', 'B07_20m',
                      'B8A_20m', 'B11_20m', 'B12_20m', 'SCL_20m', 'TCI_20m', 'WVP_20m',
                      'AOT_60m', 'B01_60m', 'B02_60m', 'B03_60m', 'B04_60m', 'B05_60m', 'B06_60m', 'B07_60m',
                      'B8A_60m', 'B09_60m', 'B11_60m', 'B12_60m', 'SCL_60m', 'TCI_60m', 'WVP_60m'
                      ]
    # create a vrt file for each band of each product to reproject products at different UTM zone
    for band in bands_list:
        logger.info('Processing band ' + str(band))
        """
        if 'S2MSI1C' in datatype:
            list_images = glob.glob(f"{job_folder}/*.SAFE/GRANULE/*/IMG_DATA/*{band}.jp2")
        elif 'S2MSI2A' in datatype:
            list_images = glob.glob(f"{job_folder}/*.SAFE/GRANULE/*/IMG_DATA/*/*{band}.jp2")
        matchname = re.search(r'\d{4}\d{2}\d{2}', list_images[0])
        logger.info("date " + str(matchname))
        cur_date = datetime.strptime(matchname.group(), '%Y%m%d').date()
        cur_date = str(cur_date).split("-")
        output_vrt = f'{job_folder}/S2B_{datatype}_{cur_date[0]}{cur_date[1]}{cur_date[2]}_{band}_{shpid}.vrt'

        for i in list_images:
            tmp_out = i.split('.jp2')
            tmp_out = tmp_out[0] + '.vrt'
            ds = gdal.Open(i)
            gdal.Warp(tmp_out, ds, dstSRS='epsg:4326', dstNodata=np.nan)
        """


        if 'S2MSI1C' in datatype:
            list_images_vrt = glob.glob(f"{job_folder}/*.SAFE/GRANULE/*/IMG_DATA/*{band}.vrt")
        elif 'S2MSI2A' in datatype:
            list_images_vrt = glob.glob(f"{job_folder}/*.SAFE/GRANULE/*/IMG_DATA/*/*{band}.vrt")

        matchname = re.search(r'\d{4}\d{2}\d{2}', list_images_vrt[0])
        logger.info("date " + str(matchname))
        cur_date = datetime.strptime(matchname.group(), '%Y%m%d').date()
        cur_date = str(cur_date).split("-")
        output_vrt = f'{job_folder}/S2B_{datatype}_{cur_date[0]}{cur_date[1]}{cur_date[2]}_{band}_{shpid}.vrt'

        # create a unique VRT file with all tiles for each band
        vrt = gdal.BuildVRT(output_vrt, list_images_vrt)
        vrt = None
    # crop merged products based on selected ROI
    for pp in Path(job_folder).rglob('S2B_*.vrt'):
        logger.info('Cropping file ' + str(pp))
        file_name = str(pp).split('.vrt')
        output_file_tmp = file_name[0]+'_tmp.tif'
        output_file = file_name[0] + '.tif'
        try:
            ds = gdal.Open(str(pp))
            es_obj = {"status": "cropping", "jobid": jobid, "orderid": orderid}
            gdal.Warp(output_file_tmp, ds,  format='GTiff', cutlineDSName=shp_file, cutlineLayer=f'shape_{shpid}',
                      cropToCutline=True, dstNodata=np.nan, callback=progress_callback, callback_data=es_obj)
            es_obj = {"status": "compressing", "jobid": jobid, "orderid": orderid}
            topts = gdal.TranslateOptions(creationOptions=['COMPRESS=LZW', 'PREDICTOR=2'],  callback=progress_callback,
                                          callback_data=es_obj)
            gdal.Translate(output_file, output_file_tmp, options=topts)
            rm_tmp = f'rm {output_file_tmp}'
            os.system(rm_tmp)
            ds = None
        except SystemError:
            logger.info('No products to be cropped. Control if original data is available.')
        except ValueError:
            logger.info("Received a NULL pointer processing: " + str(pp))
    insert_job_status = "UPDATE job SET status = %s WHERE job_id = %s"
    cursor.execute(insert_job_status, ("completed", jobid))
    conn.commit()
    conn.close()
    logger.info("Database updated with job status.")

    return jobid


