# TO make effective build container for dask to:
# docker build . -t aliaspace/nuts-dask:0.2
# docker push aliaspace/nuts-dask:0.2
# kubectl rollout restart deployments dask-scheduler dask-worker -n prefect

FROM daskdev/dask:2022.1.1-py3.8
RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ="Europe/Rome" apt-get -y install tzdata
RUN apt-get install -y wget
RUN apt-get install -y gdal-bin libgdal-dev
RUN apt-get install -y g++ git
RUN python -m pip install --upgrade pip
RUN pip install --upgrade --no-cache-dir setuptools==58.0
RUN pip install prefect==0.15.12
RUN pip install dask-kubernetes==2022.1.0
#RUN pip install git+https://github.com/dask/dask-kubernetes
RUN pip install distributed==2022.01.1
RUN pip install --global-option=build_ext --global-option="-I/usr/include/gdal" GDAL==`gdal-config --version`
RUN pip install geopandas fiona
RUN pip install psycopg2-binary
RUN pip install owslib gdal
RUN pip install bokeh
#RUN pip install fnmatch

RUN apt-get install -y curl
RUN curl -LO https://dl.k8s.io/release/v1.21.2/bin/linux/amd64/kubectl
RUN install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
RUN pip install kubernetes

RUN pip install rtree

RUN pip install google-api-python-client

#FIXME move to secret or configmap
RUN mkdir /opt/nuts/
COPY nuts-gmail.pickle /opt/nuts/

RUN apt-get remove --purge  -y g++ git
RUN rm -rf /var/lib/apt/lists/*
COPY dhusget.sh /usr/bin/dhusget.sh
RUN chmod 777 /usr/bin/dhusget.sh
