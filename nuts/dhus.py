import subprocess
import prefect
from prefect import task
import psycopg2
import psycopg2.extras
import os
import csv
import datetime
from time import sleep
import configparser
import zipfile

config_obj = configparser.ConfigParser()
config_obj.read("configfile.ini")

db_params = config_obj["postgresql"]
db_host = db_params["host"]
db_port = db_params["port"]
database = db_params["database"]
db_user = db_params["user"]
db_password = db_params["password"]

s2hub_params = config_obj["s2hub"]
s2hub_host = s2hub_params["host"]
s2hub_user = s2hub_params["user"]
s2hub_password = s2hub_params["password"]

fold_params = config_obj["res_folders"]
cache_folder = fold_params["cache_folder"]
res_folder = fold_params["result_folder"]


@task
def queryCatalog(orderid, shapeBoundaries):
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

    logger.info("Retrieving order with id: " + str(orderid))

    # read inputs from order_tracking
    order_request = "SELECT * FROM order_tracking WHERE order_id = '" + str(orderid) + "'"
    cursor.execute(order_request)
    order = cursor.fetchone()
    #order = dict(zip(zip(*cursor.description)[0], cursor.fetchone()))
    #order = dict(zip([column[0] for column in cursor.description], cursor.fetchone()))
    mission = order["source_mission"]
    data_type = order["source_data_type"]
    logger.info("Data type: " + str(mission) + " - " + str(data_type))

    # create a list of datetime between sensing_start and sensing_stop user dates
    sensing_start = order["start_time"]
    sensing_stop = order["stop_time"]
    delta = sensing_stop - sensing_start  # returns timedelta
    start_date_list = []
    stop_date_list = []
    logger.info("Querying for the time range: " + str(sensing_start) + " - " + str(sensing_stop))
    for i in range(delta.days + 1):
        day = sensing_start + datetime.timedelta(days=i)
        start_date_list.append(datetime.datetime.strftime(day, '%Y-%m-%dT00:00:00.000Z'))
        stop_date_list.append(datetime.datetime.strftime(day, '%Y-%m-%dT23:59:59.999Z'))

    product_list = []

    # Search products on catalogue for each datetime
    cursor = conn.cursor()
    total_prods = 0
    for i in range(len(start_date_list)):
        logger.info("Querying for the time range:" + str(start_date_list[i]) + " - " + str(stop_date_list[i]))

        # Use dhusget method for the query
        page = 1
        url = f'/usr/bin/dhusget.sh -d {s2hub_host} -u {s2hub_user} -p {s2hub_password} -m {mission} -S {start_date_list[i]} -E {stop_date_list[i]} -c {shapeBounds[0]},{shapeBounds[1]}:{shapeBounds[2]},{shapeBounds[3]} -T {data_type} -l 100 -P {page}'  # -q {query_result} -C {products_list} '
        logger.info(url)
        try:
            result = subprocess.check_output(url, shell=True)
            # logger.info(result)
            num_lines = sum(1 for line in open('products-list.csv'))
            total_prods += num_lines
            logger.info('Found: ' + str(num_lines) + ' on page ' + str(page))
            # update db with entries if products have been found
            if num_lines != 0:
                insert_jobs = "INSERT INTO job (order_id, start_date, end_date) VALUES (%s, %s, %s) RETURNING job_id"  # 888
                records_to_insert = (orderid, start_date_list[i], stop_date_list[i])
                cursor.execute(insert_jobs, records_to_insert)
                conn.commit()
                jobid = cursor.fetchone()[0]

                # create job folder
                job_folder = f'{res_folder}/working_dir/job_{jobid}'
                if not os.path.exists(job_folder):
                    logger.info(f"Creating folder {jobid}")
                    os.makedirs(job_folder)
                # read products from query resulting file and put them in db
                with open('products-list.csv', 'r') as prodlist:
                    csv_reader = csv.reader(prodlist, delimiter=',')
                    for row in csv_reader:
                        insert_product = "INSERT INTO source_product (job_id, uri, product_name) VALUES (%s, %s, %s) RETURNING source_product_id"
                        product_to_insert = (jobid, row[1], row[0])
                        cursor.execute(insert_product, product_to_insert)
                        conn.commit()
                        prod_index = cursor.fetchone()[0]
                        product_list.append(prod_index)

            # check if there are more than 100 products and retrieve them in case
            while num_lines > 99:
                page += 1
                url = f'/usr/bin/dhusget.sh -d {s2hub_host} -u {s2hub_user} -p {s2hub_password} -m {mission} -S {start_date_list[i]} -E {stop_date_list[i]} -c {shapeBounds[0]},{shapeBounds[1]}:{shapeBounds[2]},{shapeBounds[3]} -T {data_type} -l 100 -P {page}'  # -q {query_result} -C {products_list}'
                logger.info(url)
                result = subprocess.check_output(url, shell=True)
                # logger.info(result)
                num_lines = sum(1 for line in open('products-list.csv'))
                total_prods += num_lines
                logger.info('Found: ' + str(num_lines) + ' on page ' + str(page))
                # update db with entries if products have been found
                if num_lines != 0:
                    # read products from query resulting file and put them in db
                    with open('products-list.csv', 'r') as prodlis:
                        csv_reader = csv.reader(prodlis, delimiter=',')
                        for row in csv_reader:
                            insert_product = "INSERT INTO source_product (job_id, uri, product_name) VALUES (%s, %s, %s) RETURNING source_product_id"
                            product_to_insert = (jobid, row[1], row[0])
                            cursor.execute(insert_product, product_to_insert)
                            conn.commit()
                            prod_index = cursor.fetchone()[0]
                            product_list.append(prod_index)
        except subprocess.CalledProcessError as e:
            logger.info("Request error: " + str(e.output))

    logger.info(
        f"Found {total_prods} products on the catalogue. \n Here is the product id list: \n" + str(product_list))
    conn.close()
    # remove files in folder
    os.system("rm product*")
    os.system("rm *.xml")

    return product_list


@task
def downloadSourceProduct(sourceproductid):
    # logging connection
    logger = prefect.context.get("logger")
    logger.info(" sourceProduct: " + str(sourceproductid))

    # establish connection with db
    conn = psycopg2.connect(
        host=db_host,
        database=database,
        user=db_user,
        password=db_password,
        port=db_port
    )
    cursor = conn.cursor()

    # get the source product from source product ids
    order_request = "SELECT * FROM source_product WHERE source_product_id = '" + str(sourceproductid) + "'"
    cursor.execute(order_request)
    product = cursor.fetchone()
    jobid = product[1]
    logger.info("Checking for product: " + str(product[3]))

    # download products from catalogue
    job_folder = f'{res_folder}/working_dir/job_{jobid}'
    file_name = f'{job_folder}/{product[3]}.zip.tmp'
    file_name_def = f'{cache_folder}/{product[3]}.zip'

    logger.info("Looking for file in " + str(cache_folder))

    # if file is present in folder do not launch the wget command else do it
    if os.path.isfile(file_name_def):  # file present in cache
        logger.info("Product " + str(product[3]) + "  already downloaded!")
        res_download = 0
        download_status = 'cached'
    else:
        logger.info("Product not available yet. Downloading: " + str(product[2]))
        cmd_rm_tmp = f'rm {file_name}'
        os.system(cmd_rm_tmp)
        try:
            # checksum retrieve MD5 code (integrity of downloaded file)
            checksum_file = f'checksum_{sourceproductid}'
            cmd_csum = f'wget -nc -O {checksum_file} --user={s2hub_user} --password={s2hub_password} "{product[2]}/Checksum/Value/$value"'
            os.system(cmd_csum)
            with open(checksum_file) as check:
                check_content = check.read()
                integrity_code = check_content.split('metadata">')[1].split('</Value>')[0]
                cmd_rm = f'rm {checksum_file}'
                os.system(cmd_rm)
            os.system(cmd_rm_tmp)
            logger.info("Integrity checksum code: " + str(integrity_code))
        except OSError as exception:
            logger.info("Checksum code retrieval failed with :" + str(exception))
        # start downloading
        cmd_download = f'wget -nc -O {file_name} --user={s2hub_user} --password={s2hub_password} "{product[2]}/\$value" 2> wget_response_{sourceproductid}_job_{jobid}.txt'
        logger.info("Trying the following command: \n" + str(cmd_download))
        res_download = 1
        time_to_sleep = 10
        # while loop to execute wget and check request result
        maxretry = 30
        i_retry = 1
        download_status = 'first download instance'
        while res_download != 0:
            logger.info("*** Downloading *** \n Status: " + str(download_status))
            try:
                subprocess.run([cmd_download], shell=True)
                # os.system(cmd_download)
                with open(f"wget_response_{sourceproductid}_job_{jobid}.txt", "r") as res:
                    wget_response = res.read()
                    # logger.info("Reading wget response ... \n" + str(wget_response))

                    if '202 Accepted' in wget_response:  # offline product
                        logger.info("Reading wget response ... ")  # \n" + str(wget_response))
                        time_to_sleep = 600
                        res_download = 1
                        download_status = 'offline product'
                        logger.info("202 - " + str(download_status))
                        os.system(cmd_rm_tmp)

                    elif '503 Forbidden' in wget_response:  # Service Unavailable
                        logger.info("Reading wget response ... ")  # \n" + str(wget_response))
                        time_to_sleep = 600
                        res_download = 1
                        download_status = 'dhus service unavailable'
                        logger.info("503 - " + str(download_status))
                        os.system(cmd_rm_tmp)

                    elif '403 Forbidden' in wget_response:  # dhus quota reached
                        logger.info("Reading wget response ... ")  # \n" + str(wget_response))
                        time_to_sleep = 120
                        res_download = 1
                        download_status = 'user quota exceeded'
                        logger.info("403  - " + str(download_status))
                        os.system(cmd_rm_tmp)

                    elif '500 Internal Server Error' in wget_response:  # Unexpected nav segment Navigation Property
                        logger.info("Reading wget response ... ")  # \n" + str(wget_response))
                        time_to_sleep = 600  # TODO: forse va stoppato?
                        res_download = 1
                        download_status = 'Internal Server Error'
                        logger.info("500  - " + str(download_status))
                        os.system(cmd_rm_tmp)

                    elif os.path.isfile(file_name):  # file downloaded now
                        logger.info("Reading wget response ... ")
                        logger.info("Product " + str(product[2]) + " downloaded!")
                        logger.info("Checking for checksum integrity ... " + str(integrity_code))
                        cmd_md5 = f'md5sum {file_name} > {product[3]}.checksum'
                        os.system(cmd_md5)
                        # check integrity of downloaded product
                        with open(f"{product[3]}.checksum") as csum:
                            csum_content = csum.read()
                        if integrity_code.lower() in csum_content.lower():
                            res_download = 0
                            time_to_sleep = 0
                            download_status = 'downloaded'
                            # move zipped products in cache folder
                            cmd_move = f'mv {file_name} {file_name_def}'
                            os.system(cmd_move)
                            logger.info(
                                "Checksum passed: \n Expected code: " + str(integrity_code) + " Obtained: " + str(
                                    csum_content.split(' /usr')[0]))
                            os.system("sync")
                        else:
                            res_download = 1
                            download_status = 'MD5 failed'
                            logger.info(
                                "Checksum not passed: \n Expected code: " + str(integrity_code) + " Obtained: " + str(
                                    csum_content.split(' /usr')[0]))
                            logger.info("Remove file and trying to download it again ...")
                            os.system(cmd_rm_tmp)
                    else:
                        # TODO: what do you aspect for responses other than the above?
                        logger.info("What happened with the wget? \n" + str(wget_response))
                        download_status = 'failed'
                        res_download = 1
                        os.system(cmd_rm_tmp)

            except subprocess.CalledProcessError:
                logger.info("There was an error with the wget command ...")
                download_status = 'wget failed'
                res_download = 1
                time_to_sleep = 60
                os.system(cmd_rm_tmp)
            finally:
                logger.info("While-loop - retry num=" + str(i_retry))
                i_retry += 1
                sleep(time_to_sleep)  # wait before retry download
                if i_retry > maxretry:  # security stop: if max number of iter reached
                    logger.info("Max retry exceeded - " + str(i_retry))
                    break
        logger.info("Out of while loop  - result: " + str(res_download))

    logger.info("Job status " + str(download_status))
    # define download STATUS: downloaded or failed
    if res_download == 0:
        # clean up directory
        cmd = f'rm wget_response_{sourceproductid}_job_{jobid}.txt'
        os.system(cmd)
        """
        # unzip downloaded products belonging to the current job
        logger.info("Unzipping " + str(file_name_def))
        # cur_file = os.path.join(file_name_def)
        zip_ref = zipfile.ZipFile(file_name_def)  # create zipfile object
        zip_ref.extractall(job_folder)  # extract file to dir
        zip_ref.close()  # close file
        logger.info("Product unzipped: " + str(os.path.join(job_folder, file_name_def)))
        """

    else:
        logger.info("Problem downloading product " + str(sourceproductid))
        cmd = f'mv wget_response_{sourceproductid}_job_{jobid}.txt {cache_folder}'
        os.system(cmd)

    # update db with download status
    insert_status = "UPDATE source_product SET status = %s WHERE source_product_id = %s"
    cursor.execute(insert_status, (download_status, sourceproductid))
    conn.commit()
    conn.close()

    return sourceproductid
