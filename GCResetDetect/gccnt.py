import json
import copy
import os

from datetime import timedelta,datetime

import boto3

cloudwatch = boto3.client('cloudwatch')

emrclient = boto3.client('emr')

snsclient = boto3.client('sns')

sns_arn = os.environ['SNSALERTARN']

def send_alert_to_sns(msg):
    response = snsclient.publish(
        TopicArn=sns_arn,       
        Message=msg,
        Subject='DataNode GC count check alarm'
        )
    print(response)
    pass

def get_cluster_list():
    cluster_ids = []
    response = emrclient.list_clusters()
    if not response:
        return cluster_ids
    clusters = response.get('Clusters',[])
    if not clusters:
        return cluster_ids
    for cluster in clusters:
        cluster_ids.append(cluster.get('Id',''))
    return cluster_ids

def get_emr_instance_names_by_cluster(cluster_id):
    instance_names = []
    response = emrclient.list_instances(ClusterId=cluster_id,InstanceGroupTypes=['CORE'])
    if not response:
        return instance_names
    instances = response.get('Instances',[])
    for instance in instances:
        instance_status = instance.get('Status',{})
        if not instance_status:
            continue
        instance_state = instance_status.get('State','')
        if instance_state != 'RUNNING':
            continue
        instance_name = instance.get('PrivateDnsName','')
        instance_names.append(instance_name)
    return instance_names


base_dimensions = [
                    {
                        'Name': 'ProcessName',
                        'Value': 'DataNode-jvm',
                    },
                    {
                        'Name': 'ModelerType',
                        'Value': 'JvmMetrics',
                    }
                ]

def get_time(elem):
    return elem.get('Timestamp',None)

def get_dimensions_by_host(host_name):
    dimension_host = {}
    dimension_host['Name'] = 'Hostname'
    dimension_host['Value'] = host_name
    dimensions = copy.deepcopy(base_dimensions)
    dimensions.append(dimension_host)
    return dimensions

def get_metric_by_host(host_name):
    response  = cloudwatch.get_metric_statistics(
    Namespace='CustMetrics',
    Dimensions = get_dimensions_by_host(host_name),
    MetricName = 'GcCount',
    Period=60,
    Statistics= ['Minimum'],
    Unit='None',
    StartTime=datetime.utcnow()-timedelta(minutes=10),
    EndTime=datetime.utcnow()
    )
    datapoints = response.get('Datapoints',[])

    datapoints.sort(key=get_time)
    if len(datapoints) <3 :
        send_alert_to_sns('Not enough daata points for {}!'.format(host_name))
    old_value = datapoints[0].get('Minimum',0.0)
    recent_value = datapoints[-1].get('Minimum',0.0)
    if old_value - recent_value >0:
        send_alert_to_sns('DataNode on {} was restarted!'.format(host_name))

def lambda_handler(event, context):
    cluster_ids = get_cluster_list()
    for cluster_id in cluster_ids:
        # print(cluster_id)
        instance_names = get_emr_instance_names_by_cluster(cluster_id)
        # print(instance_names)
        for instance_name in instance_names:
            get_metric_by_host(instance_name)
    return {
        'statusCode': 200,
        'body': json.dumps('OK!')
    }
    

