import boto3

def list_all_resources():
    resources = {}

    # EC2 Instances
    ec2 = boto3.client('ec2')
    instances = ec2.describe_instances()
    resources['ec2_instances'] = [
        i['InstanceId']
        for r in instances['Reservations']
        for i in r['Instances']
    ]

    # EBS Volumes
    volumes = ec2.describe_volumes()
    resources['ebs_volumes'] = [v['VolumeId'] for v in volumes['Volumes']]

    # Elastic IPs
    addresses = ec2.describe_addresses()
    resources['elastic_ips'] = [a['PublicIp'] for a in addresses['Addresses']]

    # Security Groups
    sgs = ec2.describe_security_groups()
    resources['security_groups'] = [sg['GroupId'] for sg in sgs['SecurityGroups']]

    # Load Balancers
    elbv2 = boto3.client('elbv2')
    lbs = elbv2.describe_load_balancers()
    resources['load_balancers'] = [lb['LoadBalancerArn'] for lb in lbs['LoadBalancers']]

    # S3 Buckets
    s3 = boto3.client('s3')
    buckets = s3.list_buckets()
    resources['s3_buckets'] = [b['Name'] for b in buckets['Buckets']]

    # RDS Instances
    rds = boto3.client('rds')
    dbs = rds.describe_db_instances()
    resources['rds_instances'] = [db['DBInstanceIdentifier'] for db in dbs['DBInstances']]

    # Lambda Functions
    lam = boto3.client('lambda')
    functions = lam.list_functions()
    resources['lambda_functions'] = [fn['FunctionName'] for fn in functions['Functions']]

    # IAM Users
    iam = boto3.client('iam')
    users = iam.list_users()
    resources['iam_users'] = [u['UserName'] for u in users['Users']]

    # CloudFormation Stacks
    cfn = boto3.client('cloudformation')
    stacks = cfn.describe_stacks()
    resources['cloudformation_stacks'] = [s['StackName'] for s in stacks['Stacks']]

    # DynamoDB Tables
    dynamo = boto3.client('dynamodb')
    tables = dynamo.list_tables()
    resources['dynamodb_tables'] = tables.get('TableNames', [])

    return resources


if __name__ == "__main__":
    all_resources = list_all_resources()
    for service, items in all_resources.items():
        print(f"{service}: {items if items else 'None'}")

