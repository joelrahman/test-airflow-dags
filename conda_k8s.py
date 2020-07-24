from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.contrib.kubernetes.pod import Port

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='conda_k8s_test',
    default_args=args,
    schedule_interval=None,
    tags=['example']
)


# secret_file = Secret('volume', '/etc/sql_conn', 'airflow-secrets', 'sql_alchemy_conn')
# secret_env  = Secret('env', 'SQL_CONN', 'airflow-secrets', 'sql_alchemy_conn')
# secret_all_keys  = Secret('env', None, 'airflow-secrets-2')
# volume_mount = VolumeMount('test-volume',
#                             mount_path='/root/mount_file',
#                             sub_path=None,
#                             read_only=True)
# port = Port('http', 80)
# configmaps = ['test-configmap-1', 'test-configmap-2']

# volume_config= {
#     'persistentVolumeClaim':
#       {
#         'claimName': 'test-volume'
#       }
#     }
# volume = Volume(name='test-volume', configs=volume_config)

# affinity = {
#     'nodeAffinity': {
#       'preferredDuringSchedulingIgnoredDuringExecution': [
#         {
#           "weight": 1,
#           "preference": {
#             "matchExpressions": {
#               "key": "disktype",
#               "operator": "In",
#               "values": ["ssd"]
#             }
#           }
#         }
#       ]
#     },
#     "podAffinity": {
#       "requiredDuringSchedulingIgnoredDuringExecution": [
#         {
#           "labelSelector": {
#             "matchExpressions": [
#               {
#                 "key": "security",
#                 "operator": "In",
#                 "values": ["S1"]
#               }
#             ]
#           },
#           "topologyKey": "failure-domain.beta.kubernetes.io/zone"
#         }
#       ]
#     },
#     "podAntiAffinity": {
#       "requiredDuringSchedulingIgnoredDuringExecution": [
#         {
#           "labelSelector": {
#             "matchExpressions": [
#               {
#                 "key": "security",
#                 "operator": "In",
#                 "values": ["S2"]
#               }
#             ]
#           },
#           "topologyKey": "kubernetes.io/hostname"
#         }
#       ]
#     }
# }

# tolerations = [
#     {
#         'key': "key",
#         'operator': 'Equal',
#         'value': 'value'
#      }
# ]

run_this = BashOperator(
    task_id='run_after_loop',
    bash_command='echo 1',
    dag=dag,
)

probe = KubernetesPodOperator(dag=dag,
                              namespace='default',
                              image="joelrahman/conda-python-science",
                              # cmds=["python", "-V"],
                              cmds=["python", "-c", "'import pandas; print(pandas._version.get_versions())'"],
                              arguments=[],
                              # arguments=["'print(\"hello world\")'"],
                              # arguments=["'import pandas; print(pandas._version.get_versions())'"],
                              labels={"foo": "bar"},
                              # secrets=[secret_file, secret_env, secret_all_keys],
                              # ports=[port]
                              # volumes=[volume],
                              # volume_mounts=[volume_mount]
                              name="task-probe-pandas",
                              task_id="task-probe-pandas",
                              # affinity=affinity,
                              is_delete_operator_pod=True,
                              hostnetwork=False,
                              startup_timeout_seconds=360
                              # tolerations=tolerations,
                              # configmaps=configmaps
                              )

python_hello = KubernetesPodOperator(dag=dag,
            namespace='default',
                              image="joelrahman/conda-python-science",
                              cmds=["python", "-c", "'print(\"hello world\")'"],
                              arguments=[],
                              labels={"foo": "bar"},
                              name="task-python-hello",
                              task_id="task-python-hello",
                              is_delete_operator_pod=True,
                              hostnetwork=False,
                              startup_timeout_seconds=360
                              )

python_version = KubernetesPodOperator(dag=dag,
                              namespace='default',
                              image="joelrahman/conda-python-science",
                              cmds=["python", "-V"],
                              arguments=[],
                              labels={"foo": "bar"},
                              name="task-python-version",
                              task_id="task-python-version",
                              is_delete_operator_pod=True,
                              hostnetwork=False,
                              startup_timeout_seconds=360
                              )

probe >> run_this
python_version >> run_this
python_hello >> run_this


