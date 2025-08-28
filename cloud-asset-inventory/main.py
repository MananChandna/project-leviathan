import boto3
import json

def list_iam_roles():
    """
    Connects to AWS and lists all IAM roles in the account.
    """
    print("Starting IAM role inventory...")
    try:
        # 1. Create a boto3 client for the IAM service.
        #    By default, it uses the credentials from 'aws configure'.
        iam_client = boto3.client('iam')

        # 2. IAM can have many roles, so we use a 'paginator'.
        #    This handles multiple pages of results automatically.
        paginator = iam_client.get_paginator('list_roles')

        roles_list = []

        # 3. Iterate through each page of results.
        for page in paginator.paginate():
            for role in page['Roles']:
                # 4. For now, we'll just extract the role name and ARN.
                role_info = {
                    'RoleName': role['RoleName'],
                    'Arn': role['Arn']
                }
                roles_list.append(role_info)
                print(f"  Found role: {role['RoleName']}")

        print(f"\nTotal roles found: {len(roles_list)}")

        # 5. Save the output to a JSON file. This file will eventually
        #    be picked up by our Spark ETL job.
        with open('iam_roles.json', 'w') as f:
            json.dump(roles_list, f, indent=4)

        print("Successfully saved role inventory to iam_roles.json")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    list_iam_roles()
