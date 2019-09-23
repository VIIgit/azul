#! /usr/bin/env python3

import sys
import boto3
from azul import config


lambda_name = sys.argv[1]
role_name = config.qualified_resource_name(lambda_name)

iam = boto3.client('iam')

try:
    response = iam.get_role(RoleName=role_name)
    role = response['Role']
except iam.exceptions.NoSuchEntityException:
    role = None

try:
    policies = iam.list_role_policies(RoleName=role_name)
except iam.exceptions.NoSuchEntityException:
    policies = None

# print('Role: ', role, '\n')
# print('Policies:', policies, '\n')

if policies is not None:
    for policy_name in policies['PolicyNames']:
        try:
            print(f"Deleting Policy {policy_name}")
            # iam.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
        except:
            print(f"Error deleting Policy {policy_name}")

if role is not None:
    try:
        print(f"Deleting Role {role_name}")
        # iam.delete_role(RoleName=role_name)
    except:
        print(f"Error deleting Role {role_name}")
