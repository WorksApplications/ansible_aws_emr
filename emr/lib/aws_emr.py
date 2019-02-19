#!/usr/bin/python
# This file is a self designed Ansible Module to manage AWS Elastic MapReduce.

ANSIBLE_METADATA = {
    'metadata_version': '1.1',
    'status': ['preview'],
    'supported_by': 'community'
}

DOCUMENTATION = '''
---
module: aws_emr
short_description: Manage AWS EMR
description:
    - "This is my longer description explaining my sample module"
version_added: "2.5.5"
author: "Mengfan Shan (fox)"
options:
    aws_access_key:
      description:
        - The AWS Accees Key to create/manager EMR cluster
      required: true
      default: null
    aws_secret_key:
      description:
        - The AWS Secret Key to create/manager EMR cluster
      required: true
      default: null
    region:
      description:
        - The AWS Region to create EMR or the operated EMR belongs. Valid values are ['us-east-1', 'us-west-2', 'us-west-1', 'eu-west-1', 'eu-central-1', 'ap-southeast-1', 'ap-northeast-1', 'ap-southeast-2', 'ap-northeast-2', 'ap-south-1', 'sa-east-1']
      required: true
      default: null
    mode:
      description:
        - Given operation to EMR, valid values are ['create', 'describe', 'get-cluster-id', 'get-cluster-ids', 'terminate', 'terminate-all', 'check-status', 'get-master-ip', 'get-core-ips', 'get-slave-ips', 'get-collection-id-by-name', 'add-instance-group', 'scale-out', 'scale-in']
      required: true
    cluster_name:
      description:
        - Cluster Name to find/operator EMR. There should be only one active EMR cluster for the given name if cluster_name is used to operator.
      required: false
      default: null
    id:
      description:
        - Cluster id of target cluster. If cluster id is not given. Cluster name is also acceptable, but only one active cluster could be found.
      required: false
      default: null
    auto_scaling_role:
      description:
        - the auto scaling role to create EMR cluster
      required: false
      default: 'EMR_AutoScaling_DefaultRole'
    applications:
      description:
        - the applications we need to create an EMR cluster
      required: false
      default: ['Hadoop', 'Spark']
    log_url:
      description:
        - log url to keep logs of EMR clusters and batches running on EMR
      required: false
      default: null
    release_label:
      description:
        - release_label is the version of EMR
      required: false
      default: 'emr-5.8.0'
    service_role:
      description:
        - the iam role of EMR cluster
      required: false
      default: 'iam-role-emr'
    ec2_service_role:
      description:
        - the iam role of EMR ec2 instances
      required: false
      default: 'iam-role-emr-ec2'
    scale_down_behavior:
      description:
        - the scale down behavior when an instance is scaled in
      required: false
      default: 'TERMINATE_AT_TASK_COMPLETION'
    tags:
      description:
        - tags for ec2 instances when create an EMR cluster
      required: false
      default: [{'Name': 'EMR_test_fox'}]
    ec2_key_file_name:
      description:
        - the pem file name for ssh access to EMR instance
      required: false
    emr_master_security_group:
      description:
        - security group id for EMR master
      required: false
    emr_slave_security_group:
      description:
        - security group id for EMR slave
      required: false
    emr_service_security_group:
      description:
        - security group id for emr service access
      required: false
    key_alive_when_no_steps:
      description:
        - If EMR is alive after steps are done
      required: false
      default: True
    termination_protection:
      description:
        - If we need terminate protection for created EMR cluster
      required: false
      default: True
    ec2_subnet:
      description:
        - Subnet Id to create an EMR cluster
      required: false
      default: True
    ec2_subnets:
      description:
        - A list of subnet ids to create an EMR cluster,
      required: false
      default: True
    bootstrap_actions:
      description:
        - A path to a json file for bootstracp actions.
      required: false
    configurations:
      description:
        - A path to a json file for application(Hadoop/Spark) configurations.
      required: false
    instances:
      description:
        - A path to a json file for instance properties.
      required: false
    instance_collection_name:
      description:
        - Name for instance group or instance fleet
      required: false
    instance_group_id:
      description:
        - ID of instance group
      required: false
    scale_out_instance_type:
      description:
        - Instance type for scale out instance group.
      required: false
    scale_out_instance_count:
      description:
        - Instance count for scale out instance group/fleet.
      required: false
    enable_fleet:
      description:
        - If you need to create an EMR cluster by instance fleet or instance group
      required: false
    scale_in_instance_count:
      description:
        - Number of instances need to be scaled in. If it was more than current, it will be scaled in to 0. If the value is
      required: false
'''

EXAMPLES = '''
# Example 1: Create EMR cluster by instance groups
- name: create emr cluster by instance groups
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: create
    name: EMR_exmaple_name
    auto_scaling_role: EMR_AutoScaling_DefaultRole
    applications: ['Hadoop', 'Spark']
    log_url: 's3://emr-bucket-example/logs'
    tags: [{'Name': 'EMR_exmaple_name'}]
    ec2_key_file_name: key-exmaple
    emr_master_security_group: 'sg-example1'
    emr_slave_security_group: 'sg-example2'
    emr_service_security_group: 'sg-example3'
    instances: "{{ playbook_dir }}/roles/emr/init-create/files/instances.json"
    bootstrap_actions: "{{ playbook_dir }}/roles/emr/init-create/files/bootstrapactions.json"
    configurations: "{{ playbook_dir }}/roles/emr/init-create/files/emr_config.json"
    ec2_subnet: 'subnet-example'

# Example 2: Create EMR cluster by instance fleets
- name: create emr cluster by instance fleets, cross different AZs.
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: create
    name: EMR_exmaple_name
    auto_scaling_role: EMR_AutoScaling_DefaultRole
    applications: ['Hadoop', 'Spark']
    log_url: 's3://emr-bucket-example/logs'
    tags: [{'Name': 'EMR_exmaple_name'}]
    ec2_key_file_name: key-exmaple
    emr_master_security_group: 'sg-example1'
    emr_slave_security_group: 'sg-example2'
    emr_service_security_group: 'sg-example3'
    instances: "{{ playbook_dir }}/roles/emr/init-create-fleet/files/instances.json"
    bootstrap_actions: "{{ playbook_dir }}/roles/emr/init-create-fleet/files/bootstrapactions.json"
    configurations: "{{ playbook_dir }}/roles/emr/init-create-fleet/files/emr_config.json"
    ec2_subnets: ['subnet-example1', 'subnet-example2']
    enable_fleet: true

# Example 3: Describe cluster by cluster Id
- name: Describe cluster by cluster Id
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: describe
    id: "{{ CLUSTER_ID }}"

# Example 4: Get cluster Id of the active cluster by cluster name
- name: Get cluster Id of the active cluster by cluster name
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: get-cluster-id
    name: "{{ CLSUTER_NAME }}"

# Example 5: Get cluster Id of all active clusters by cluster name
- name: Get cluster Id of all active clusters by cluster name
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: get-cluster-ids
    name: "{{ CLSUTER_NAME }}"

# Example 6: Terminate EMR by cluster Id
- name: Terminate EMR by cluster Id
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: terminate
    id: "{{ CLUSTER_ID }}"

# Example 7: Terminate EMR by cluster name. There should be only one active cluster found by the given cluster name
- name: Terminate EMR by cluster name
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: terminate
    name: "{{ CLSUTER_NAME }}"

# Example 8: Terminate all active cluster by given cluster name
- name: Terminate all active cluster by given cluster name
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: terminate-all
    name: "{{ CLSUTER_NAME }}"

# Example 9: Check EMR clusters by cluster Id
- name: Check EMR clusters by cluster Id
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: check-status
    id: "{{ CLUSTER_ID }}"

# Example 10: Get the IP address of Master instance by cluster Id. Work for both Instance groups and instance fleets.
- name: get IP address of master instance
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: get-master-ip
    id: "{{ CLUSTER_ID }}"

# Example 11: Get the IP address of Master instance by cluster name. Work for both Instance groups and instance fleets.
- name: get IP address of master instance
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: get-master-ip
    name: "{{ CLSUTER_NAME }}"

# Example 12: Get the IP address of CORE instances by cluster Id. Work for both Instance groups and instance fleets.
- name: get IP address of CORE instances
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: get-core-ips
    id: "{{ CLUSTER_ID }}"

# Example 13: Get the IP address of CORE instances by cluster name. Work for both Instance groups and instance fleets.
- name: get IP address of CORE instances
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: get-core-ips
    name: "{{ CLSUTER_NAME }}"

# Example 14: Get the IP address of Slave instances by cluster Id. Work for both Instance groups and instance fleets.
- name: get IP address of TASK instances
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: get-slave-ips
    id: "{{ CLUSTER_ID }}"

# Example 15: Get the IP address of Slave instances by cluster name. Work for both Instance groups and instance fleets.
- name: get IP address of TASK instances
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: get-slave-ips
    name: "{{ CLSUTER_NAME }}"

# Example 16: Get instance group/fleet id by cluster id and instance group/fleet name
- name: Get instance group/fleet id by name
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: get-collection-id-by-name
    id: "{{ CLUSTER_ID }}"
    instance_collection_name: "{{ INSTANCE_GROUP_NAME }}"

# Example 17: Get instance group/fleet id by cluster name and instance group/fleet name
- name: Get instance group/fleet id by name
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: get-collection-id-by-name
    name: "{{ CLSUTER_NAME }}"
    instance_collection_name: "{{ INSTANCE_GROUP_NAME }}"

# Example 18: Add an instance group for a given EMR cluster Id (Not used). EMR cluster name is also acceptable.
- name: Add an instance group
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: add-instance-group
    id: "{{ CLUSTER_ID }}"
    scale_out_instance_type: m4.large
    scale_out_instance_count: 4
    instance_collection_name: 'New_EMR_Task_IG'

# Example 18: Add an instance group for a given EMR cluster Id (Not used). EMR cluster name is also acceptable.
- name: Add an instance group
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: add-instance-group
    id: "{{ CLUSTER_ID }}"
    scale_out_instance_type: m4.large
    scale_out_instance_count: 4
    instance_collection_name: 'New_EMR_Task_IG'

# Example 19: Scale out on instance fleet. The example will add one on-demand request on Task Instance Fleet
- name: Scale out on instance fleet
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: scale-out
    id: "{{ CLUSTER_ID }}"
    scale_out_instance_count: 1

# Example 19: Scale out an instance group by instance group name. The number of instance for the given instance group will be resize to 1.
- name: Scale out on instance group
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: scale-out
    id: "{{ CLUSTER_ID }}"
    scale_out_instance_count: 1
    instance_collection_name: "{{ INSTANCE_GROUP_NAME }}"

# Example 20: Scale out by adding a new instance group, if the given instance group by name was not found.
- name: Scale out on instance group
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: scale-out
    id: "{{ CLUSTER_ID }}"
    scale_out_instance_count: 1
    instance_collection_name: "{{ INSTANCE_GROUP_NAME }}"
    scale_out_instance_type: r4.xlarge

# Example 21: Scale in on instance fleet. The on-demand target will be reduced by 1. (0 is the minimum)
- name: Scale in
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: scale-in
    id: "{{ CLUSTER_ID }}"
    scale_in_instance_count: 1

# Example 22: Scale in on instance fleet. The on-demand target will be reduced by 1. (0 is the minimum) If scale_in_instance_count is undefined, the on-demand target will be set to 0 directly.
- name: Scale in
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: scale-in
    id: "{{ CLUSTER_ID }}"
    scale_in_instance_count: 1

# Example 22: Scale in on instance fleet. The on-demand target will be reduced by 1. (0 is the minimum) If scale_in_instance_count is undefined, the on-demand target will be set to 0 directly.
- name: Scale in
  aws_emr:
    aws_access_key: "{{ AWS_ACCESS_KEY }}"
    aws_secret_key: "{{ AWS_SECURITY_KEY }}"
    region: "{{ AWS_REGION }}"
    mode: scale-in
    id: "{{ CLUSTER_ID }}"
    scale_in_instance_count: 1


'''

import os
import json
from time import sleep

try:
    import boto3
    from botocore.exceptions import ClientError
    import botocore
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

def convert_application(applications):
    result = []
    for application in applications:
        result.append({'Name':application})
    return result

def convert_tag(tags):
    result = []
    for tag in tags:
        result.append({'Key':tag.items()[0][0], 'Value':tag.items()[0][1]})
    return result

def is_instance_fleet_enalbed(cluster):
    if cluster.get('InstanceCollectionType') == 'INSTANCE_FLEET':
        return True
    return False

def get_client(region, access_key, secret_key, security_token):
    if access_key == None or access_key == '':
        return boto3.client(
            'emr',
            region_name=region
        )
    else:
        return boto3.client(
            'emr',
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=security_token
        )

def list_active_clusters_by_name(emr_client, cluster_name):
    i = 0.1
    max_interval = 30
    cluster_list = []
    while i < max_interval:
        try:
            cluster_list = emr_client.list_clusters(
                ClusterStates=[
                    'STARTING',
                    'BOOTSTRAPPING',
                    'RUNNING',
                    'WAITING',
                ]
            ).get('Clusters')
            break
        except ClientError as err:
            if err.response['Error']['Message'] == "Rate exceeded":
                sleep(i)
                i = i * 2
                if i > max_interval:
                    raise
            else:
                raise
    result = []
    for cluster in cluster_list:
        if cluster.get('Name') == cluster_name:
            result.append(cluster)
    return result

def list_terminated_clusters_by_name(emr_client, cluster_name):
    i = 0.1
    max_interval = 30
    cluster_list = []
    while i < max_interval:
        try:
            cluster_list = emr_client.list_clusters(
                ClusterStates=[
                    'TERMINATING',
                    'TERMINATED',
                    'TERMINATED_WITH_ERRORS',
                ]
            ).get('Clusters')
            break
        except ClientError as err:
            if err.response['Error']['Message'] == "Rate exceeded":
                sleep(i)
                i = i * 2
                if i > max_interval:
                    raise
            else:
                raise
    result = []
    for cluster in cluster_list:
        if cluster.get('Name') == cluster_name:
            result.append(cluster)
    return result

def list_all_clusters_by_name(emr_client, cluster_name):
    i = 0.1
    max_interval = 30
    cluster_list = []
    while i < max_interval:
        try:
            cluster_list = emr_client.list_clusters().get('Clusters')
            break
        except ClientError as err:
            if err.response['Error']['Message'] == "Rate exceeded":
                sleep(i)
                i = i * 2
                if i > max_interval:
                    raise
            else:
                raise
    result = []
    for cluster in cluster_list:
        if cluster.get('Name') == cluster_name:
            result.append(cluster)
    return result

def terminate_emr(emr_client, cluster_id):
    try:
        emr_client.set_termination_protection(
            JobFlowIds=[
                cluster_id,
            ],
            TerminationProtected=False
        )
        emr_client.terminate_job_flows(
            JobFlowIds=[
                cluster_id,
            ]
        )
        return "Terminate Cluster ID: " + cluster_id + "request is successfully submitted."
    except ClientError as e:
        return "Termiantion failed due to " + e.response["Error"]["Code"]

def get_cluster_ids(emr_client, cluster_name):
    cluster_list = list_active_clusters_by_name(emr_client, cluster_name)
    result = []
    for cluster in cluster_list:
        if cluster.get('Name') == cluster_name:
            result.append(cluster.get('Id'))
    return result

def get_cluster_id(emr_client, cluster_name):
    id_list = get_cluster_ids(emr_client, cluster_name)
    if len(id_list) == 0:
        return None
    return id_list[0]

def describle_cluster(emr_client, cluster_id):
    i = 0.1
    max_interval = 30
    while i < max_interval:
        try:
            return emr_client.describe_cluster(
                ClusterId = cluster_id
            ).get('Cluster')
        except ClientError as err:
            if err.response['Error']['Message'] == "Rate exceeded":
                sleep(i)
                i = i * 2
                if i > max_interval:
                    raise
            else:
                raise

def list_instance_groups(emr_client, cluster_id):
    i = 0.1
    max_interval = 30
    while i < max_interval:
        try:
            return emr_client.list_instance_groups(
                ClusterId=cluster_id
            ).get('InstanceGroups')
        except ClientError as err:
            if err.response['Error']['Message'] == "Rate exceeded":
                sleep(i)
                i = i * 2
                if i > max_interval:
                    raise
            else:
                raise

def list_instance_fleets(emr_client, cluster_id):
    i = 0.1
    max_interval = 30
    while i < max_interval:
        try:
            return emr_client.list_instance_fleets(
                ClusterId=cluster_id
            ).get('InstanceFleets')
        except ClientError as err:
            if err.response['Error']['Message'] == "Rate exceeded":
                sleep(i)
                i = i * 2
                if i > max_interval:
                    raise
            else:
                raise

def list_instance_by_group_id(emr_client, cluster_id, instance_group_id):
    i = 0.1
    max_interval = 30
    while i < max_interval:
        try:
            return emr_client.list_instances(
                ClusterId=cluster_id,
                InstanceGroupId=instance_group_id,
                InstanceStates=['BOOTSTRAPPING', 'RUNNING']
            ).get("Instances")
        except ClientError as err:
            if err.response['Error']['Message'] == "Rate exceeded":
                sleep(i)
                i = i * 2
                if i > max_interval:
                    raise
            else:
                raise

def list_instance_by_group_type(emr_client, cluster_id, instance_group_type):
    i = 0.1
    max_interval = 30
    while i < max_interval:
        try:
            return emr_client.list_instances(
                ClusterId=cluster_id,
                InstanceGroupTypes=instance_group_type,
                InstanceStates=['BOOTSTRAPPING', 'RUNNING']
            ).get("Instances")
        except ClientError as err:
            if err.response['Error']['Message'] == "Rate exceeded":
                sleep(i)
                i = i * 2
                if i > max_interval:
                    raise
            else:
                raise

def list_instance_by_fleet_type(emr_client, cluster_id, instance_fleet_type):
    i = 0.1
    max_interval = 30
    while i < max_interval:
        try:
            return emr_client.list_instances(
                ClusterId=cluster_id,
                InstanceFleetType=instance_fleet_type,
                InstanceStates=['BOOTSTRAPPING', 'RUNNING']
            ).get("Instances")
        except ClientError as err:
            if err.response['Error']['Message'] == "Rate exceeded":
                sleep(i)
                i = i * 2
                if i > max_interval:
                    raise
            else:
                raise

def get_private_ip_address(instance_detail_list):
    result = []
    for instance_detail in instance_detail_list:
        result.append(instance_detail.get('PrivateIpAddress'))
    return result

def get_cluster_state(cluster):
    return cluster.get('Status').get('State')

def get_cluster_state_change_reason(cluster):
    return cluster.get('Status').get('StateChangeReason').get('Message')

def add_scale_out_instance_group(emr_client, cluster_id, instance_group_name, scale_out_instance_type, scale_out_instance_count):
    return emr_client.add_instance_groups(
        InstanceGroups=[
            {
                'Name': instance_group_name,
                'Market': 'ON_DEMAND',
                'InstanceRole': 'TASK',
                'InstanceType': scale_out_instance_type,
                'InstanceCount': scale_out_instance_count
            }
        ],
        JobFlowId = cluster_id
    )

def resize_instance_group(emr_client, cluster_id, instance_group_id, instance_count):
    return emr_client.modify_instance_groups(
        InstanceGroups = [
            {
                'InstanceGroupId': instance_group_id,
                'InstanceCount': instance_count
            }
        ],
        ClusterId = cluster_id
    )

def resize_instance_fleet(emr_client, cluster_id, instance_fleet_id, instance_count, spot_count):
    return emr_client.modify_instance_fleet(
        InstanceFleet = {
            'InstanceFleetId': instance_fleet_id,
            'TargetOnDemandCapacity': instance_count,
            'TargetSpotCapacity': spot_count
        },
        ClusterId = cluster_id
    )


from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils._text import to_bytes, to_native

def run_module():
    # define the available arguments/parameters that a user can pass to
    # the module
    module_args = dict(
        aws_access_key = dict(type='str', required=True, no_log=True),
        aws_secret_key = dict(type='str', required=True, no_log=True),
        security_token = dict(type='str', required=False, no_log=True),
        region = dict(choices=['us-east-1', 'us-west-2', 'us-west-1', 'eu-west-1', 'eu-central-1', 'ap-southeast-1', 'ap-northeast-1', 'ap-southeast-2', 'ap-northeast-2', 'ap-south-1', 'sa-east-1'], required=True),
        mode = dict(choices=['create', 'describe', 'get-cluster-id', 'get-cluster-ids', 'terminate', 'terminate-all', 'check-status', 'get-master-ip', 'get-core-ips', 'get-slave-ips', 'get-collection-id-by-name', 'add-instance-group', 'scale-out', 'scale-in', 'active-instances-by-collection'], required=True),
        name = dict(type='str'),
        id = dict(type='str'),
        auto_scaling_role = dict(type='str', default='EMR_AutoScaling_DefaultRole'),
        applications = dict(type='list', default=['Hadoop', 'Spark']),
        log_url = dict(type='str'),
        release_label = dict(type='str', default='emr-5.8.0'),
        service_role = dict(type='str', default='iam-role-emr'),
        ec2_service_role = dict(type='str', default='iam-role-emr-ec2'),
        scale_down_behavior = dict(type='str', default='TERMINATE_AT_TASK_COMPLETION'),
        tags = dict(type='list'),
        ec2_key_file_name = dict(type='str', default=''),
        emr_master_security_group = dict(type='str'),
        emr_slave_security_group = dict(type='str'),
        emr_service_security_group = dict(type='str'),
        key_alive_when_no_steps = dict(type='bool', default=True),
        termination_protection = dict(type='bool', default=True),
        ec2_subnet = dict(type='str'),
        ec2_subnets = dict(type='list'),
        bootstrap_actions = dict(type='path'),
        configurations = dict(type='path'),
        instances = dict(type='path'),
        instance_collection_name = dict(type='str'),
        instance_group_id = dict(type='str'),
        scale_out_instance_type = dict(type='str', default='m4.large'),
        scale_out_instance_count = dict(type='int', default=1),
        scale_in_instance_count = dict(type='int'),
        enable_fleet = dict(type='bool', default=False)
    )

    result = dict(
        changed=False
    )

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    if module.check_mode:
        return result

    if not HAS_BOTO3:
        module.fail_json(msg='boto3 and botocore required for this module')

    #Prepare params
    aws_access_key = module.params.get('aws_access_key')
    aws_secret_key = module.params.get('aws_secret_key')
    security_token = module.params.get('security_token')
    region = module.params.get('region')
    mode = module.params.get('mode')
    name = module.params.get('name')
    id = module.params.get('id')
    auto_scaling_role = module.params.get('auto_scaling_role')
    applications = module.params.get('applications')
    log_url = module.params.get('log_url')
    release_label = module.params.get('release_label')
    service_role = module.params.get('service_role')
    ec2_service_role = module.params.get('ec2_service_role')
    scale_down_behavior = module.params.get('scale_down_behavior')
    tags = module.params.get('tags')
    ec2_key_file_name = module.params.get('ec2_key_file_name')
    emr_master_security_group = module.params.get('emr_master_security_group')
    emr_slave_security_group = module.params.get('emr_slave_security_group')
    emr_service_security_group = module.params.get('emr_service_security_group')
    key_alive_when_no_steps = module.params.get('key_alive_when_no_steps')
    termination_protection = module.params.get('termination_protection')
    ec2_subnet = module.params.get('ec2_subnet')
    ec2_subnets = module.params.get('ec2_subnets')
    bootstrap_actions = module.params.get('bootstrap_actions')
    configurations = module.params.get('configurations')
    instances = module.params.get('instances')
    instance_collection_name = module.params.get('instance_collection_name')
    instance_group_id = module.params.get('instance_group_id')
    scale_out_instance_type = module.params.get('scale_out_instance_type')
    scale_out_instance_count = module.params.get('scale_out_instance_count')
    scale_in_instance_count = module.params.get('scale_in_instance_count')
    enable_fleet = module.params.get('enable_fleet')

    emr_client = get_client(region, aws_access_key, aws_secret_key, security_token)

    if mode == 'create':
        #Validation Check
        if name in ('', None):
            module.fail_json(msg='name is required to create a cluster')

        if log_url in ('', None):
            module.fail_json(msg='log_url is required to create a cluster')

        if ec2_key_file_name in ('', None):
            module.fail_json(msg='ec2_key_file_name is required to create a cluster')

        if emr_master_security_group in ('', None):
            module.fail_json(msg='emr_master_security_group is required to create a cluster')

        if emr_slave_security_group in ('', None):
            module.fail_json(msg='emr_slave_security_group is required to create a cluster')

        if emr_service_security_group in ('', None):
            module.fail_json(msg='emr_service_security_group is required to create a cluster')

        if (ec2_subnet in ('', None)) and (ec2_subnets is None or ec2_subnets == []):
            module.fail_json(msg='ec2_subnet or ec2_subnets is required to create a cluster')

        if instances is ('', None):
            module.fail_json(msg='instances is required to create a cluster')

        application_list = convert_application(applications)

        tag_list = convert_tag(tags)

        emr_config = []
        if configurations not in ('', None):
            config_file_path = to_bytes(configurations, errors='surrogate_or_strict')
            with open(config_file_path) as json_file:
                emr_config = json.load(json_file)

        bootstrap_list = []
        if bootstrap_actions not in ('', None):
            bootstrap_file_path = to_bytes(bootstrap_actions, errors='surrogate_or_strict')
            with open(bootstrap_file_path) as boot_file:
                bootstrap_list = json.load(boot_file)

        instance_config = {}
        instance_list = []
        instance_file_path = to_bytes(instances, errors='surrogate_or_strict')
        with open(instance_file_path) as instance_file:
            instance_list = json.load(instance_file)
        if enable_fleet:
            instance_config['InstanceFleets'] = instance_list
            instance_config['Ec2SubnetIds'] = ec2_subnets
        else:
            instance_config['InstanceGroups'] = instance_list
            instance_config['Ec2SubnetId'] = ec2_subnet
        instance_config['Ec2KeyName'] = ec2_key_file_name
        instance_config['ServiceAccessSecurityGroup'] = emr_service_security_group
        instance_config['EmrManagedMasterSecurityGroup'] = emr_master_security_group
        instance_config['EmrManagedSlaveSecurityGroup'] = emr_slave_security_group
        instance_config['KeepJobFlowAliveWhenNoSteps'] = key_alive_when_no_steps
        instance_config['TerminationProtected'] = termination_protection

        if enable_fleet:
            cluster_id = emr_client.run_job_flow(
                Applications=application_list,
                Name=name,
                LogUri=log_url,
                ReleaseLabel=release_label,
                ServiceRole=service_role,
                JobFlowRole=ec2_service_role,
                BootstrapActions=bootstrap_list,
                Configurations=emr_config,
                ScaleDownBehavior=scale_down_behavior,
                Instances=instance_config,
                VisibleToAllUsers=True,
                Tags=tag_list
            ).get("JobFlowId")
        else:
            cluster_id = emr_client.run_job_flow(
                AutoScalingRole=auto_scaling_role,
                Applications=application_list,
                Name=name,
                LogUri=log_url,
                ReleaseLabel=release_label,
                ServiceRole=service_role,
                JobFlowRole=ec2_service_role,
                BootstrapActions=bootstrap_list,
                Configurations=emr_config,
                ScaleDownBehavior=scale_down_behavior,
                Instances=instance_config,
                VisibleToAllUsers=True,
                Tags=tag_list
            ).get("JobFlowId")

        result['changed'] = True
        result['id'] = cluster_id

    if mode == 'describe':
        cluster = describle_cluster(emr_client, id)
        if cluster is not None:
            result['cluster'] = cluster
            result['changed'] = True

    if mode == 'get-cluster-id':
        if name in ('', None):
            module.exit_json(msg="Cluster Name is needed to get ID of active clusters.")
        id = get_cluster_id(emr_client, name)
        result['changed'] = True
        result['id'] = id

    if mode == 'get-cluster-ids':
        if name in ('', None):
            module.exit_json(msg="Cluster Name is needed to get ID of active clusters.")
        id_list = get_cluster_ids(emr_client, name)
        if len(id_list) == 0:
            module.exit_json(msg='Not active cluster by name: ' + name + ' was found')
        result['changed'] = True
        result['id'] = id_list

    if mode == 'terminate':
        if id in ('', None):
            if name in ('', None):
                module.fail_json(msg='Cluster name or id is required to terminate')
            id = get_cluster_id(emr_client, name)
            if id is None:
                module.fail_json(msg='No active EMR cluster was founded by name: ' + name + '.' )
        result['response'] = terminate_emr(emr_client, id)
        result['changed'] = True
        result['cluster_id'] = id

    if mode == 'terminate-all':
        if name in ('', None):
            module.fail_json(msg='Cluster name is required to terminate')
        id_list = get_cluster_ids(emr_client, name)
        for cluster_id in id_list:
            terminate_emr(emr_client, cluster_id)
        result['changed'] = True
        result['id'] = id_list

    if mode == 'check-status':
        cluster = describle_cluster(emr_client, id)
        if cluster is not None:
            result['changed'] = True
            result['state'] = get_cluster_state(cluster)
            result['state_changed_reason'] = get_cluster_state_change_reason(cluster)
        else:
            result['msg'] = "Cluster of given cluster ID was not found."

    if mode == 'get-master-ip':
        if id in ('', None):
            if name in ('', None):
                module.fail_json(msg='name or id is required to get master ip')
            id = get_cluster_id(emr_client, name)
            if id is None:
                module.fail_json(msg='No active EMR cluster was founded by name: ' + name + '.' )
        cluster = describle_cluster(emr_client, id)
        if cluster is None:
            module.exit_json(msg='No active EMR cluster was founded by Id: ' + id + '.' )
        if is_instance_fleet_enalbed(cluster):
            master_instance_list = list_instance_by_fleet_type(emr_client, id, 'MASTER')
        else:
            master_instance_list = list_instance_by_group_type(emr_client, id, ['MASTER'])
        if master_instance_list is not None:
            private_ip_list = get_private_ip_address(master_instance_list)
            if len(private_ip_list) > 0:
                result['master_private_ip'] = private_ip_list[0]

    if mode == 'get-core-ips':
        if id in ('', None):
            if name in ('', None):
                module.fail_json(msg='name or id is required to get core ips')
            id = get_cluster_id(emr_client, name)
            if id is None:
                module.fail_json(msg='No active EMR cluster was founded by name: ' + name + '.' )
        cluster = describle_cluster(emr_client, id)
        if cluster is None:
            module.exit_json(msg='No active EMR cluster was founded by Id: ' + id + '.' )
        if is_instance_fleet_enalbed(cluster):
            core_instance_list = list_instance_by_fleet_type(emr_client, id, 'CORE')
        else:
            core_instance_list = list_instance_by_group_type(emr_client, id, ['CORE'])
        if core_instance_list is not None:
            result['core_private_ips'] = get_private_ip_address(core_instance_list)

    if mode == 'get-slave-ips':
        if id in ('', None):
            if name in ('', None):
                module.fail_json(msg='name or id is required to get task ips')
            id = get_cluster_id(emr_client, name)
            if id is None:
                module.fail_json(msg='No active EMR cluster was founded by name: ' + name + '.' )
        cluster = describle_cluster(emr_client, id)
        if cluster is None:
            module.exit_json(msg='No active EMR cluster was founded by Id: ' + id + '.' )
        if is_instance_fleet_enalbed(cluster):
            slave_instance_list = list_instance_by_fleet_type(emr_client, id, 'TASK') + list_instance_by_fleet_type(emr_client, id, 'CORE')
        else:
            slave_instance_list = list_instance_by_group_type(emr_client, id, ['CORE', 'TASK'])
        if slave_instance_list is not None:
            result['slave_private_ips'] = get_private_ip_address(slave_instance_list)

    if mode == 'get-collection-id-by-name':
        if id in ('', None):
            if name in ('', None):
                module.fail_json(msg='name or id is required to get group id')
            id = get_cluster_id(emr_client, name)
            if id is None:
                module.fail_json(msg='No active EMR cluster was founded by name: ' + name + '.' )

        if instance_collection_name in ('', None):
            module.fail_json(msg='instance_collection_name is required to get instance collection(group/fleet) id')
        cluster = describle_cluster(emr_client, id)
        if cluster is None:
            module.exit_json(msg='No active EMR cluster was founded by Id: ' + id + '.' )
        if is_instance_fleet_enalbed(cluster):
            instance_collection_list = list_instance_fleets(emr_client, id)
        else:
            instance_collection_list = list_instance_groups(emr_client, id)
        for instance_collection in instance_collection_list:
            if instance_collection.get('Name') == instance_collection_name:
                instance_collection_id = instance_collection.get('Id')
        if instance_group_id in ('', None):
            module.exit_json(msg='No instance collection was found', **result)
        result['instance_collection_id'] = instance_collection_id
        result['changed'] = True

    if mode == 'add-instance-group':
        if id in ('', None):
            if name in ('', None):
                module.fail_json(msg='name or id is required to add an instance group to scale out')
            id = get_cluster_id(emr_client, name)
            if id is None:
                module.fail_json(msg='No active EMR cluster was founded by name: ' + name + '.' )
        result['msg'] = add_scale_out_instance_group(emr_client, id, instance_collection_name, scale_out_instance_type, scale_out_instance_count)
        result['changed'] = True

    if mode == 'scale-out':
        if id in ('', None):
            if name in ('', None):
                module.fail_json(msg='cluster name or cluster id is required to scale out')
            id = get_cluster_id(emr_client, name)
            if id is None:
                module.fail_json(msg='No active EMR cluster was founded by name: ' + name + '.' )

        cluster = describle_cluster(emr_client, id)
        if cluster is None:
            module.exit_json(msg='No active EMR cluster was founded by Id: ' + id + '.' )

        if is_instance_fleet_enalbed(cluster):
            # For instance fleet
            instance_collection_list = list_instance_fleets(emr_client, id)
            instance_collection_id = ''
            for instance_collection in instance_collection_list:
                if instance_collection.get('InstanceFleetType') == 'TASK':
                    instance_collection_id = instance_collection.get('Id')
                    target_on_demand_capacity = instance_collection.get('TargetOnDemandCapacity')
                    target_spot_capacity = instance_collection.get('TargetSpotCapacity')
            if instance_collection_id in ('', None):
                module.exit_json(msg="Task Instance Fleet does not exists.")
            result['target_instance_count'] = target_on_demand_capacity + scale_out_instance_count
            result['response'] = resize_instance_fleet(emr_client, id, instance_collection_id, scale_out_instance_count + target_on_demand_capacity, target_spot_capacity)
            result['operation'] = "Instance group exists. Resize"

        else:
            # For instance group
            instance_collection_list = list_instance_groups(emr_client, id)
            instance_collection_id = ''
            for instance_collection in instance_collection_list:
                if instance_collection.get('Name') == instance_collection_name:
                    instance_collection_id = instance_collection.get('Id')
            if instance_collection_id in ('', None):
                result['response'] = add_scale_out_instance_group(emr_client, id, instance_collection_name, scale_out_instance_type, scale_out_instance_count)
                result['operation'] = "Instance collection does not exists. Add new instance collection to scale out"
            else:
                result['response'] = resize_instance_group(emr_client, id, instance_collection_id, scale_out_instance_count)
                result['operation'] = "Instance group exists. Resize"
            result['changed'] = True

    if mode == 'scale-in':
        if id in ('', None):
            if name in ('', None):
                module.fail_json(msg='cluster name or cluster id is required to scale in')
            id = get_cluster_id(emr_client, name)
            if id is None:
                module.fail_json(msg='No active EMR cluster was founded by name: ' + name + '.' )

        cluster = describle_cluster(emr_client, id)
        if cluster is None:
            module.exit_json(msg='No active EMR cluster was founded by Id: ' + id + '.' )

        if is_instance_fleet_enalbed(cluster):
            # For instance fleet
            instance_collection_list = list_instance_fleets(emr_client, id)
            instance_collection_id = ''
            for instance_collection in instance_collection_list:
                if instance_collection.get('InstanceFleetType') == 'TASK':
                    instance_collection_id = instance_collection.get('Id')
                    target_on_demand_capacity = instance_collection.get('TargetOnDemandCapacity')
                    target_spot_capacity = instance_collection.get('TargetSpotCapacity')
            if instance_collection_id in ('', None):
                module.exit_json(msg="Task Instance Fleet does not exists.")
            if scale_in_instance_count is None:
                final_target = 0
            else:
                final_target = target_on_demand_capacity - scale_in_instance_count
                if final_target < 0:
                    final_target = 0
            result['target_instance_count'] = final_target
            result['response'] = resize_instance_fleet(emr_client, id, instance_collection_id, final_target, target_spot_capacity)
            result['operation'] = "Instance group exists. Resize"

        else:
            #For instance group
            instance_group_list = list_instance_groups(emr_client, id)
            instance_group_id = ''
            for instance_group in instance_group_list:
                if instance_group.get('Name') == instance_collection_name:
                    instance_group_id = instance_group.get('Id')
            if instance_group_id in ('', None):
                module.exit_json(msg='The instance group to scale in does not exist', **result)
            result['response'] = resize_instance_group(emr_client, id, instance_group_id, 0)
            result['operation'] = "Instance group exists. Resize to 0"
            result['changed'] = True

    if mode == 'active-instances-by-collection':
        if id in ('', None):
            if name in ('', None):
                module.fail_json(msg='cluster name or cluster id is required to get active instances by collection id.')
            id = get_cluster_id(emr_client, name)
            if id is None:
                module.fail_json(msg='No active EMR cluster was founded by name: ' + name + '.' )

        cluster = describle_cluster(emr_client, id)
        if cluster is None:
            module.exit_json(msg='No active EMR cluster was founded by Id: ' + id + '.' )

        if is_instance_fleet_enalbed(cluster):
            # For instance fleet
            instance_collection_list = list_instance_fleets(emr_client, id)
            for instance_collection in instance_collection_list:
                if instance_collection.get('InstanceFleetType') == 'TASK':
                    result['active_instance_count'] = instance_collection.get('ProvisionedOnDemandCapacity')
        else:
            # For instance group
            instance_collection_list = list_instance_groups(emr_client, id)
            for instance_collection in  instance_collection_list:
                if instance_collection.get('Name') == instance_collection_name:
                    result['active_instance_count'] = instance_collection.get('RunningInstanceCount')
        result['changed'] = True

    module.exit_json(**result)

def main():
    run_module()

if __name__ == '__main__':
    main()