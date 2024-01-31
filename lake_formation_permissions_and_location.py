import boto3
from botocore.exceptions import ClientError

iam = boto3.client('iam')
lf = boto3.client('lakeformation')
policy_arn = 'arn:aws:iam::aws:policy/AWSLakeFormationDataAdmin'
iam_user = 'BillyBob'

#This method attaches the data lake admin policy to the specified IAM user
iam.attach_user_policy(
   UserName=iam_user,
   PolicyArn=policy_arn
)

# Check if the policy has been successfully attached
response = iam.list_attached_user_policies(UserName=iam_user)
attached_policies = [policy['PolicyArn'] for policy in response['AttachedPolicies']]
if policy_arn in attached_policies:
    print("The policy has been successfully attached.")
else:
    print("The policy has not been attached.")

# This registers an S3 bucket as a resource in Lake Formation
try:
    lf.register_resource(
       ResourceArn='arn:aws:s3:::joes-cap-stone-bucket/nwis/',
       RoleArn='arn:aws:iam::459793645210:role/aws-service-role/lakeformation.amazonaws.com/AWSServiceRoleForLakeFormationDataAccess'
    )
except ClientError as e:
    if e.response['Error']['Code'] == 'AlreadyExistsException':
        print("The resource is already registered.")
    else:
        print(f"Unexpected error: {e}")