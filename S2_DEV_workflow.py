from prefect import Flow, Parameter, flatten
from prefect.storage import Docker
from prefect.run_configs import KubernetesRun
from prefect.executors import LocalDaskExecutor, DaskExecutor

from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.triggers import all_successful, any_failed

from nuts import common, dhus, s2
from prefect.tasks.prefect import create_flow_run

import configparser

config_obj = configparser.ConfigParser()
config_obj.read("configfile.ini")

run_params = config_obj["run"]
run_machine = run_params["machine"]
run_docker = run_params["repo_url"]

prefect_params = config_obj["prefect"]
prefect_project_name = prefect_params["project_name"]
prefect_s2_prefix = prefect_params["s2_WF_prefix"]

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

with Flow(prefect_s2_prefix+"_success", storage=STORAGE, run_config=RUN_CONFIG, executor=EXECUTOR) as successFlow:
    orderid = Parameter("orderid")
    common.log_message(" Success order: " + str(orderid) + " !!")

with Flow(prefect_s2_prefix+"_clean", storage=STORAGE, run_config=RUN_CONFIG, executor=EXECUTOR) as cleanFlow:
    orderid = Parameter("orderid")
    common.cleanup(orderid)

with Flow(prefect_s2_prefix+"_job", storage=STORAGE, run_config=RUN_CONFIG, executor=EXECUTOR) as jobFlow:
    orderid = Parameter("orderid")
    common.updateTimeStamp(orderid, "ts_elaboration_t2")
    shapeBoundaries = common.retrieveShape(orderid)
    source_products = dhus.queryCatalog(orderid, shapeBoundaries)
    downloaded_products = dhus.downloadSourceProduct.map(flatten(source_products))
    jobs = common.organizeJobs(downloaded_products, orderid)
    result = s2.mergeProducts.map(flatten(jobs))
    common.updateTimeStamp(orderid, "ts_elaboration_t3", upstream_tasks=[result])
    common.notifyResult(result, orderid)

with Flow(prefect_s2_prefix+"_workflow", storage=STORAGE, run_config=RUN_CONFIG,  executor=EXECUTOR) as flow:
    orderid = Parameter("orderid")

    message_task = common.log_message(" Processing order: " + str(orderid) + " !!")

    S2_job_f = create_flow_run(
        flow_name=prefect_s2_prefix+"_job",
        project_name=prefect_project_name,
        parameters=dict(orderid=orderid)
    )
    S2_job_wf = wait_for_flow_run(
        S2_job_f, raise_final_state=True, stream_logs=True
    )

    # on failure of S2_DEV_job_wf:
    S2_clean_f = create_flow_run(
        flow_name=prefect_s2_prefix+"_clean",
        project_name=prefect_project_name,
        upstream_tasks=[S2_job_wf],
        task_args=dict(name="Flow run on Failure", trigger=any_failed),
        parameters=dict(orderid=orderid)
    )
    S2_clean_wf = wait_for_flow_run(
        S2_clean_f, raise_final_state=False, stream_logs=True
    )

    # on success of conditional_run:
    S2_success_f = create_flow_run(
        flow_name=prefect_s2_prefix+"_success",
        project_name=prefect_project_name,
        upstream_tasks=[S2_job_wf],
        task_args=dict(name="Flow run on Success", trigger=all_successful),
        parameters=dict(orderid=orderid)
    )
    S2_success_fw = wait_for_flow_run(
        S2_success_f, raise_final_state=True, stream_logs=True
    )

    #raise S2_DEV_success_fw.state
    # if S2_DEV_success_fw.state == True:
    #     raise signals.SUCCESS()
    # else:
    #     raise signals.FAIL()

    flow.set_reference_tasks([S2_success_fw])


# if run_machine == 'local':
#     flow.executor = LocalDaskExecutor()
#     flow.register(project_name="NUTS", labels=["local"])
# elif run_machine == 'cluster':
#     flow.run_config = KubernetesRun(
#         #job_template_path="./spec.yaml",
#         image_pull_secrets="regcred",
#         env={"PREFECT__CLOUD__HEARTBEAT_MODE": "thread", "PREFECT__CLOUD__API_KEY": "XXXXXXXXXX:4200"}
#     )
#     flow.executor=DaskExecutor(
#         #cluster_class=lambda: KubeCluster(make_pod_spec(image=prefect.context.image, memory_limit='4G', memory_request='4G', cpu_limit=1, cpu_request=1)),
#         #adapt_kwargs={"minimum": 2, "maximum": 4},
#         "tcp://dask-scheduler:8786"
#     )
#     flow.register(project_name="NUTS", labels=["k8s"])

flow.register(project_name=prefect_project_name, labels=["k8s"])


