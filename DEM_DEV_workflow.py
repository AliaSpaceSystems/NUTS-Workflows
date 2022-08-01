from prefect import Flow, Parameter, flatten
from prefect.executors import LocalDaskExecutor, DaskExecutor
from prefect.storage import Docker
from prefect.run_configs import KubernetesRun
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.triggers import any_failed, all_successful

from nuts import common, dem

import configparser

config_obj = configparser.ConfigParser()
config_obj.read("configfile.ini")

run_params = config_obj["run"]
run_machine = run_params["machine"]
run_docker = run_params["repo_url"]

prefect_params = config_obj["prefect"]
prefect_project_name = prefect_params["project_name"]
prefect_dem_prefix = prefect_params["dem_WF_prefix"]

STORAGE = Docker(
    dockerfile="Dockerfile",
    registry_url=run_docker
)

RUN_CONFIG = KubernetesRun(
    #job_template_path="./spec.yaml",
    image_pull_secrets="regcred",
    env={"PREFECT__CLOUD__HEARTBEAT_MODE": "thread", "PREFECT__CLOUD__API_KEY": "XXXXXXXXXX:4200"}
)

EXECUTOR = DaskExecutor(
    "tcp://dask-scheduler:8786"
)

with Flow(prefect_dem_prefix+"_success", storage=STORAGE, run_config=RUN_CONFIG, executor=EXECUTOR) as successFlow:
    orderid = Parameter("orderid")
    common.log_message(" Success order: " + str(orderid) + " !!")

with Flow(prefect_dem_prefix+"_clean", storage=STORAGE, run_config=RUN_CONFIG, executor=EXECUTOR) as cleanFlow:
    orderid = Parameter("orderid")
    common.cleanup(orderid)

with Flow(prefect_dem_prefix+"_job", storage=STORAGE, run_config=RUN_CONFIG, executor=EXECUTOR) as jobFlow:
    orderid = Parameter("orderid")
    common.updateTimeStamp(orderid, "ts_elaboration_t2")
    shapeBoundaries = common.retrieveShape(orderid)
    downloaded_products = dem.downloadDEMproduct(orderid, shapeBoundaries)
    jobs = common.organizeJobs(downloaded_products, orderid)
    result = dem.cropDEMproducts.map(flatten(jobs))
    common.updateTimeStamp(orderid, "ts_elaboration_t3", upstream_tasks=[result])
    common.notifyResult(result, orderid)

with Flow(prefect_dem_prefix+"_workflow", storage=STORAGE, run_config=RUN_CONFIG,  executor=EXECUTOR) as flow:
    orderid = Parameter("orderid")

    message_task = common.log_message(" Processing order: " + str(orderid) + " !!")

    dem_job_f = create_flow_run(
        flow_name=prefect_dem_prefix+"_job",
        project_name=prefect_project_name,
        parameters=dict(orderid=orderid)
    )
    dem_job_wf = wait_for_flow_run(
        dem_job_f, raise_final_state=True, stream_logs=True
    )

    # on failure of S2_DEV_job_wf:
    dem_clean_f = create_flow_run(
        flow_name=prefect_dem_prefix+"_clean",
        project_name=prefect_project_name,
        upstream_tasks=[dem_job_wf],
        task_args=dict(name="Flow run on Failure", trigger=any_failed),
        parameters=dict(orderid=orderid)
    )
    dem_clean_wf = wait_for_flow_run(
        dem_clean_f, raise_final_state=False, stream_logs=True
    )

    # on success of conditional_run:
    dem_success_f = create_flow_run(
        flow_name=prefect_dem_prefix+"_success",
        project_name=prefect_project_name,
        upstream_tasks=[dem_job_wf],
        task_args=dict(name="Flow run on Success", trigger=all_successful),
        parameters=dict(orderid=orderid)
    )
    dem_success_fw = wait_for_flow_run(
        dem_success_f, raise_final_state=True, stream_logs=True
    )

    flow.set_reference_tasks([dem_success_fw])

flow.register(project_name=prefect_project_name, labels=["k8s"])

