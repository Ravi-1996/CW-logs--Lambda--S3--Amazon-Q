import boto3

def list_unused_resources():
    unused = {}

    ec2 = boto3.client('ec2')

    # Unattached EBS Volumes
    volumes = ec2.describe_volumes(Filters=[{'Name': 'status', 'Values': ['available']}])
    unused['unattached_volumes'] = [v['VolumeId'] for v in volumes['Volumes']]

    # Unassociated Elastic IPs
    addresses = ec2.describe_addresses()
    unused['unassociated_eips'] = [a['PublicIp'] for a in addresses['Addresses'] if 'InstanceId' not in a]

    # Unused Security Groups (no ENI attached)
    sgs = ec2.describe_security_groups()['SecurityGroups']
    enis = ec2.describe_network_interfaces()['NetworkInterfaces']
    attached_sg_ids = {sg for eni in enis for sg in eni['Groups']}
    unused['unused_security_groups'] = [
        sg['GroupId'] for sg in sgs
        if sg['GroupId'] not in attached_sg_ids and sg['GroupName'] != 'default'
    ]

    # Unused Elastic Load Balancers
    elbv2 = boto3.client('elbv2')
    lbs = elbv2.describe_load_balancers()['LoadBalancers']
    unused['unused_load_balancers'] = [lb['LoadBalancerArn'] for lb in lbs if not lb['AvailabilityZones']]

    # Unused Snapshots owned by account
    sts = boto3.client('sts')
    account_id = sts.get_caller_identity()['Account']
    snapshots = ec2.describe_snapshots(OwnerIds=[account_id])['Snapshots']
    unused['unused_snapshots'] = [snap['SnapshotId'] for snap in snapshots if 'VolumeId' not in snap]

    return unused


if __name__ == "__main__":
    resources = list_unused_resources()
    for k, v in resources.items():
        print(f"{k}: {v if v else 'None'}")

