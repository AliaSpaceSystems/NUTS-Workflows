from prefect import Flow, Parameter, flatten
from prefect.executors import LocalDaskExecutor, DaskExecutor
from prefect.storage import Docker
from prefect.run_configs import KubernetesRun

from nuts import common, dem

import configparser

config_obj = configparser.ConfigParser()
config_obj.read("configfile.ini")

run_params = config_obj["run"]
run_machine = run_params["machine"]
run_docker = run_params["repo_url"]

with Flow("DEM_workflow") as flow:
    orderid = Parameter("orderid")
    shapeBoundaries = common.retrieveShape(orderid)
    downloaded_products = dem.downloadDEMproduct(orderid, shapeBoundaries)
    jobs = common.organizeJobs(downloaded_products, orderid)
    result = dem.cropDEMproducts.map(flatten(jobs))
    common.notifyResult(result, orderid)

flow.storage = Docker(
    dockerfile="Dockerfile",
    registry_url=run_docker
)

if run_machine == 'local':
    flow.executor = LocalDaskExecutor()
    flow.register(project_name="NUTS", labels=["local"])
elif run_machine == 'cluster':
    flow.run_config = KubernetesRun(
        #job_template_path="./spec.yaml",
        image_pull_secrets="regcred",
        env={"PREFECT__CLOUD__HEARTBEAT_MODE": "thread"}
    )
    flow.executor=DaskExecutor(
        #cluster_class=lambda: KubeCluster(make_pod_spec(image=prefect.context.image, memory_limit='4G', memory_request='4G', cpu_limit=1, cpu_request=1)),
        #adapt_kwargs={"minimum": 2, "maximum": 4},
        "tcp://dask-scheduler:8786"
    )
    flow.register(project_name="NUTS", labels=["k8s"])


