##################################
How to set up the NUTS environment
##################################

The Alia-Space NUTS project provides an online tool to allow users obtaining merged and cropped data by selecting
a data product, a Region of Interest (ROI), and a time period.

Set up the CONDA environment
----------------------------


To work with NUTS, start by creating a new conda environment. Let's call it `nuts_env`, but
any valid name would do (change the following instructions accordingly):

    conda create -n nuts_env python=3.8 geopandas owslib

Activate the environment:

    conda activate nuts_env

Once the environment is activated, install the required libraries to work with Prefect and the Postgres DB:

    conda install -y -c conda-forge prefect psycopg2

You are almost ready to execute your flow!

In the following paragraphs, we show you how to start a Prefect flow and an example of a request.

Prefect set up
--------------
Open a terminal and navigate to the repository folder. You should see the `docker-compose.yaml`, the `create_db.sh`,
and the `SENTINEL_workflow.py`/`DEM_workflow.py` files in the current folder. If so, you are in the right place.

Once there, you have to execute the following commands:

    docker-compose up

Open a new terminal (the docker output occupies the previous one) and go to the same folder.
The first time you start up the system, execute the following commands:

    ./create_db.sh

    prefect server create-tenant --name NUTS_tenant

Finally, go to the page `localhost:8080` in a local browser and create a new project in the Prefect UI.
Call the `new project` `NUTS`.

Now, you are ready to execute the last few commands in a terminal, pointing to the repository path.

First, register the workflows on the prefect server:

    python S2_workflow.py


    python DEM_workflow.py


    python S2EST_workflow.py


    python DEMEST_workflow.py

Second, start the prefect agent using the following command:

    prefect agent docker start -l local --network prefect-server

All you have to do now is connect to the DB to fill your request!

**NOTE:** if you want to run the DEM flow, use the following command instead of `python NUTS_workflow.py`:

    python DEM_workflow.py

Connect to the DB
-----------------

Open the Postgres IDE and connect to the NUTS DB.

    name: nuts_db
    host: postgres
    port: 5432
    username: nuts_user
    password: nuts_password

Once connected, you should see the following tables:
    * user
    * order_table
    * job
    * source_product


Start the areaSelector client
-----------------------------

In the repository folder, run the following command from a terminal shell:

    npm install

    npm install  -g  @angular/cli

You are now ready to start the server

    ng serve --port 9091

To open the *areaSelector* client, go to http:localhost:9091


Case study
----------

Sentinel-2 run
~~~~~~~~~~~~~~

To enable a request in the Prefect flow, you must fill the `user` table in the DB.
Specifically:

    *user*:
        user_id (a unique number is automatically assigned for each DB row inserting the name)

        name

        email address


Go to http:localhost:9091 for the *areaSelector* client. Select a Layer data and level (for example, *NUTS* and *NUTS_2*) from the burger menu on the top left side.
Hover over the map and select a region by clicking on the area of interest (for example, Lazio).

Enable the data selection window by clicking on the top right menu.
Select a value for each required field:

    Source: Sentinel2

    Data: S2MSI1C

    Time: 09/01/2021 - 09/05/2021


Finally, click on the **Estimate** button to receive and email with the details of the data found and the link to execute the actual run.
This request will make you download 28 products of Sentinel-2 data.

Conversely, you can execute the run directly, by clicking on the **Execute directly** button.

DEM run
~~~~~~~

To request DEM data, follow the example below.

In the database, if any, define a user:

    *user*:
        user_id (a unique number is automatically assigned for each DB row inserting the name)

        name

        email address


Go to http:localhost:9091 for the *areaSelector* client. Select a Layer data and level (for example, *GADM* and *GADM_1*)
from the burger menu on the top left side.
Hover over the map and select a region by clicking on the area of interest (for example, Kebili in Africa continent).

Enable the data selection window by clicking on the top right menu.
Select a value for each required field:

    Source: DEM

    Data: DS

    Time: 09/01/2020 - 09/05/2020

Finally, click on the **Estimate** button to receive and email with the details of the data found and the link to execute the actual run.
This request will make you download 1 product of DEM data.

Conversely, you can execute the run directly, by clicking on the **Execute directly** button.

Have fun!!




