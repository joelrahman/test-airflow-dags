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
    dag_id='fire-data-processing',
    default_args=args,
    schedule_interval=None,
    tags=['afms']
)

env_vars = {
    'SSH_CRED':'/gadi/id_rsa_gadi',
    'WALD_SETTINGS':'/opt/fire/config/defaults-mk.json'
}

data_key = Secret('volume', '/gadi_orig', 'gadi','id_rsa_gadi')
data_id = Secret('volume','/etc/ssh/gadi-id','gadi-id','fingerprint')
config_file = Secret('volume','/opt/fire/config','fmc-config','defaults-mk.json')
secrets = [data_key,data_id,config_file]
# secret_env  = Secret('env', 'SQL_CONN', 'airflow-secrets', 'sql_alchemy_conn')
# secret_all_keys  = Secret('env', None, 'airflow-secrets-2')
volume_mount = VolumeMount('g-data',
                            mount_path='/g/data',
                            sub_path=None,
                            read_only=False)
# port = Port('http', 80)
# configmaps = ['test-configmap-1', 'test-configmap-2']

volume_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'g-data'
      }
    }

pod_volume = Volume(name='g-data', configs=volume_config)

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

AU_TILES = [
    "h27v11", "h27v12", "h28v11", "h28v12",
    "h28v13", "h29v10", "h29v11", "h29v12",
]
#     "h29v13", "h30v10", "h30v11", "h30v12",
#     "h31v10", "h31v11", "h31v12", "h32v10",
#     "h32v11"
# ]

run_this = BashOperator(
    task_id='run_after_loop',
    bash_command='echo 1',
    dag=dag,
)

debug_task_one = KubernetesPodOperator(dag=dag,
                                namespace='default',
                                image="anuwald/fire-data-processing",
                                cmds=["ls", "-lh","/gadi","/etc/ssh","/opt/fire/config"],
                                # cmds=["python", "update_fmc.py","-t",tile,"-y","2020","-dst","/g/data/fmc_%s.nc"%tile],
                                arguments=[],
                                labels={"foo": "bar"},
                                env_vars=env_vars,
                                secrets=secrets,
                                # ports=[port]
                                volumes=[pod_volume],
                                volume_mounts=[volume_mount],
                                name='debug_task_one',
                                task_id='debug_task_one',
                                # affinity=affinity,
                                is_delete_operator_pod=True,
                                hostnetwork=False,
                                startup_timeout_seconds=360
                                # tolerations=tolerations,
                                # configmaps=configmaps
                                )

debug_task_one >> run_this


debug_task_one = KubernetesPodOperator(dag=dag,
                                namespace='default',
                                image="anuwald/fire-data-processing",
                                cmds=["env"],
                                # cmds=["python", "update_fmc.py","-t",tile,"-y","2020","-dst","/g/data/fmc_%s.nc"%tile],
                                arguments=[],
                                labels={"foo": "bar"},
                                env_vars=env_vars,
                                secrets=secrets,
                                # ports=[port]
                                volumes=[pod_volume],
                                volume_mounts=[volume_mount],
                                name='debug_task_two',
                                task_id='debug_task_two',
                                # affinity=affinity,
                                is_delete_operator_pod=True,
                                hostnetwork=False,
                                startup_timeout_seconds=360
                                # tolerations=tolerations,
                                # configmaps=configmaps
                                )

debug_task_one >> run_this

fmc_mosaic_task_name="DUMMY_update_fmc_mosaic"
fmc_mosaic_task = KubernetesPodOperator(dag=dag,
                                namespace='default',
                                image="anuwald/fire-data-processing",
                                cmds=["python", "-c","print('Dummy task - pretending to process FMC mosaic')"],
                                # cmds=["python", "update_fmc.py","-t",tile,"-y","2020","-dst","/g/data/fmc_%s.nc"%tile],
                                arguments=[],
                                labels={"foo": "bar"},
                                env_vars=env_vars,
                                secrets=secrets,
                                # ports=[port]
                                volumes=[pod_volume],
                                volume_mounts=[volume_mount],
                                name=fmc_mosaic_task_name,
                                task_id=fmc_mosaic_task_name,
                                # affinity=affinity,
                                is_delete_operator_pod=True,
                                hostnetwork=False,
                                startup_timeout_seconds=360
                                # tolerations=tolerations,
                                # configmaps=configmaps
                                )

flam_mosaic_task_name="DUMMY_update_flammability_mosaic"
flam_mosaic_task = KubernetesPodOperator(dag=dag,
                                namespace='default',
                                image="anuwald/fire-data-processing",
                                cmds=["python", "-c","print('Dummy task - pretending to process Flammability mosaic')"],
                                # cmds=["python", "update_fmc.py","-t",tile,"-y","2020","-dst","/g/data/fmc_%s.nc"%tile],
                                arguments=[],
                                labels={"foo": "bar"},
                                env_vars=env_vars,
                                secrets=secrets,
                                # ports=[port]
                                volumes=[pod_volume],
                                volume_mounts=[volume_mount],
                                name=flam_mosaic_task_name,
                                task_id=flam_mosaic_task_name,
                                # affinity=affinity,
                                is_delete_operator_pod=True,
                                hostnetwork=False,
                                startup_timeout_seconds=360
                                # tolerations=tolerations,
                                # configmaps=configmaps
                                )

fmc_mosaic_task >> run_this
flam_mosaic_task >> run_this

for tile in AU_TILES:
    fmc_task_name="update_fmc_%s"%tile
    fmc_commands = [
        "ln -s /etc/ssh/gadi-id/fingerprint /etc/ssh/ssh_known_hosts",
        "mkdir /gadi",
        "cp /gadi_orig/id_rsa_gadi /gadi",
        "chmod 600 /gadi/id_rsa_gadi",
        "python update_fmc.py -t %s -d 2020 -dst /g/data/fmc_%s.nc -tmp /tmp"%(tile,tile)
    ]
    fmc_task = KubernetesPodOperator(dag=dag,
                                 namespace='default',
                                 image="anuwald/fire-data-processing",
                                 cmds=[
                                     "/bin/bash","-c",
                                     " && ".join(fmc_commands)
                                 ],
                                #  cmds=[
                                #      "python", "update_fmc.py",
                                #      "-t",tile,
                                #      "-d","2020",
                                #      "-dst","/g/data/fmc_%s.nc"%tile,
                                #      "-tmp","/tmp"],
                                 arguments=[],
                                 labels={"foo": "bar"},
                                 env_vars=env_vars,
                                 secrets=secrets,
                                 # ports=[port]
                                 volumes=[pod_volume],
                                 volume_mounts=[volume_mount],
                                 name=fmc_task_name,
                                 task_id=fmc_task_name,
                                 # affinity=affinity,
                                 is_delete_operator_pod=True,
                                 hostnetwork=False,
                                 startup_timeout_seconds=360
                                 # tolerations=tolerations,
                                 # configmaps=configmaps
                                 )
    fmc_task >> fmc_mosaic_task

    flam_task_name="DUMMY_update_flam_%s"%tile
    flam_task = KubernetesPodOperator(dag=dag,
                                 namespace='default',
                                 image="anuwald/fire-data-processing",
                                #  cmds=["python", "update_flammability.py","-t",tile,"-y","2020","-dst","/g/data/fmc_%s.nc"%tile],
                                 cmds=["python", "-c","print('Dummy task - pretending to process flammability for tile %s')"%tile],
                                 arguments=[],
                                 labels={"foo": "bar"},
                                 env_vars=env_vars,
                                 secrets=secrets,
                                 # ports=[port]
                                 volumes=[pod_volume],
                                 volume_mounts=[volume_mount],
                                 name=flam_task_name,
                                 task_id=flam_task_name,
                                 # affinity=affinity,
                                 is_delete_operator_pod=True,
                                 hostnetwork=False,
                                 startup_timeout_seconds=360
                                 # tolerations=tolerations,
                                 # configmaps=configmaps
                                 )

    fmc_task >> flam_task
    flam_task >> flam_mosaic_task

