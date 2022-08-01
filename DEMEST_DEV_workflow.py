from prefect import Flow, Parameter
from prefect.executors import LocalDaskExecutor, DaskExecutor
from prefect.storage import Docker
from prefect.run_configs import KubernetesRun
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.triggers import any_failed, all_successful

from nuts import common, estimator

import configparser

config_obj = configparser.ConfigParser()
config_obj.read("configfile.ini")

run_params = config_obj["run"]
run_machine = run_params["machine"]
run_docker = run_params["repo_url"]

prefect_params = config_obj["prefect"]
prefect_project_name = prefect_params["project_name"]
prefect_demest_prefix = prefect_params["demEst_WF_prefix"]

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

with Flow(prefect_demest_prefix+"_success", storage=STORAGE, run_config=RUN_CONFIG, executor=EXECUTOR) as successFlow:
    orderid = Parameter("orderid")
    common.log_message(" Success order: " + str(orderid) + " !!")

with Flow(prefect_demest_prefix+"_clean", storage=STORAGE, run_config=RUN_CONFIG, executor=EXECUTOR) as cleanFlow:
    orderid = Parameter("orderid")
    common.cleanup(orderid)

with Flow(prefect_demest_prefix+"_job", storage=STORAGE, run_config=RUN_CONFIG, executor=EXECUTOR) as jobFlow:
    orderid = Parameter("orderid")
    common.updateTimeStamp(orderid, "ts_estimation_t2")
    shapeBoundaries = common.retrieveShape(orderid)
    estimation = estimator.demestimator(orderid, shapeBoundaries)
    common.updateTimeStamp(orderid, "ts_estimation_t3", upstream_tasks=[estimation])
    estimator.notifyDemEstResult(orderid, estimation)

with Flow(prefect_demest_prefix+"_workflow", storage=STORAGE, run_config=RUN_CONFIG,  executor=EXECUTOR) as flow:
    orderid = Parameter("orderid")

    message_task = common.log_message(" Processing order: " + str(orderid) + " !!")

    demEst_job_f = create_flow_run(
        flow_name=prefect_demest_prefix+"_job",
        project_name=prefect_project_name,
        parameters=dict(orderid=orderid)
    )
    demEst_job_wf = wait_for_flow_run(
        demEst_job_f, raise_final_state=True, stream_logs=True
    )

    # on failure of S2_DEV_job_wf:
    demEst_clean_f = create_flow_run(
        flow_name=prefect_demest_prefix+"_clean",
        project_name=prefect_project_name,
        upstream_tasks=[demEst_job_wf],
        task_args=dict(name="Flow run on Failure", trigger=any_failed),
        parameters=dict(orderid=orderid)
    )
    demEst_clean_wf = wait_for_flow_run(
        demEst_clean_f, raise_final_state=False, stream_logs=True
    )

    # on success of conditional_run:
    demEst_success_f = create_flow_run(
        flow_name=prefect_demest_prefix+"_success",
        project_name=prefect_project_name,
        upstream_tasks=[demEst_job_wf],
        task_args=dict(name="Flow run on Success", trigger=all_successful),
        parameters=dict(orderid=orderid)
    )
    demEst_success_fw = wait_for_flow_run(
        demEst_success_f, raise_final_state=True, stream_logs=True
    )

    flow.set_reference_tasks([demEst_success_fw])

flow.register(project_name=prefect_project_name, labels=["k8s"])

