import boto3

region = 'ap-northeast-2'
tag = {'Schedule': 'true'}
ec2 = boto3.client('ec2', region_name=region)


def get_instances_to_toggle():
    try:
        response = ec2.describe_instances(Filters=[
            {'Name': f'tag:{key}', 'Values': [value]} for key, value in tag.items()
        ])
        return [
            {'InstanceId': instance['InstanceId'],
             'State': instance['State']['Name']}
            for reservation in response.get('Reservations', [])
            for instance in reservation.get('Instances', [])
        ]
    except Exception as e:
        print(f"Error getting instances: {e}")
        return []


def toggle_instance_state(instance):
    instance_id = instance['InstanceId']
    current_state = instance['State']

    try:
        if current_state == 'stopped':
            ec2.start_instances(InstanceIds=[instance_id])
            print(f'Successfully started instance: {instance_id}')
        elif current_state == 'running':
            ec2.stop_instances(InstanceIds=[instance_id])
            print(f'Successfully stopped instance: {instance_id}')
        else:
            print(f"Invalid instance state for {instance_id}: {current_state}")
    except Exception as e:
        print(f"Error toggling instance state for {instance_id}: {e}")


def lambda_handler(event, context):
    try:
        instances_to_toggle = get_instances_to_toggle()
        for instance in instances_to_toggle:
            toggle_instance_state(instance)
    except Exception as e:
        print(f"Lambda execution error: {e}")
