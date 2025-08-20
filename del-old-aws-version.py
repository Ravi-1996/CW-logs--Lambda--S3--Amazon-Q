import boto3

def delete_old_s3_versions(bucket_name, keep_latest=1):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_object_versions')

    for page in paginator.paginate(Bucket=bucket_name):
        versions = page.get('Versions', [])
        # group by object key
        files = {}
        for v in versions:
            key = v['Key']
            files.setdefault(key, []).append(v)

        for key, vlist in files.items():
            # sort versions by LastModified (latest first)
            vlist.sort(key=lambda x: x['LastModified'], reverse=True)
            old_versions = vlist[keep_latest:]

            for ov in old_versions:
                print(f"Deleting old S3 version: {key} - {ov['VersionId']}")
                s3.delete_object(Bucket=bucket_name, Key=key, VersionId=ov['VersionId'])


def delete_old_lambda_versions(function_name, keep_latest=1):
    lam = boto3.client('lambda')
    versions = lam.list_versions_by_function(FunctionName=function_name)['Versions']

    # skip $LATEST
    versions = [v for v in versions if v['Version'] != '$LATEST']
    versions.sort(key=lambda x: int(x['Version']), reverse=True)

    old_versions = versions[keep_latest:]
    for ov in old_versions:
        print(f"Deleting old Lambda version: {function_name} - {ov['Version']}")
        lam.delete_function(FunctionName=function_name, Qualifier=ov['Version'])


def delete_old_ecr_images(repository_name, keep_latest=5):
    ecr = boto3.client('ecr')
    images = ecr.list_images(repositoryName=repository_name, filter={'tagStatus': 'TAGGED'})['imageIds']
    if not images:
        return

    # Describe images to get push timestamps
    details = ecr.describe_images(repositoryName=repository_name, imageIds=images)['imageDetails']
    details.sort(key=lambda x: x['imagePushedAt'], reverse=True)

    old_images = details[keep_latest:]
    for img in old_images:
        print(f"Deleting old ECR image: {repository_name}:{img.get('imageTags', ['<untagged>'])}")
        ecr.batch_delete_image(repositoryName=repository_name, imageIds=[{'imageDigest': img['imageDigest']}])

def delete_old_amis(keep_latest=2):
    ec2 = boto3.client('ec2')
    sts = boto3.client('sts')
    account_id = sts.get_caller_identity()['Account']

    images = ec2.describe_images(Owners=[account_id])['Images']
    images.sort(key=lambda x: x['CreationDate'], reverse=True)

    old_images = images[keep_latest:]
    for img in old_images:
        print(f"Deregistering old AMI: {img['ImageId']} ({img['Name']})")
        ec2.deregister_image(ImageId=img['ImageId'])


if __name__ == "__main__":
    # Example usage
    delete_old_s3_versions(bucket_name="my-versioned-bucket", keep_latest=2)
    delete_old_lambda_versions(function_name="MyLambdaFunction", keep_latest=2)
    delete_old_ecr_images(repository_name="my-app-repo", keep_latest=3)
    delete_old_amis(keep_latest=3)

