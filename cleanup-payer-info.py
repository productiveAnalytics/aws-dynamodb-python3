#!/usr/bin/python

import sys
import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import csv

def main():
    real_args = sys.argv[1:]  # 1st arg is always the program name, so skip it
    args_count = len(real_args)
    print('Number of arguments:', args_count, 'arguments.')
    print('Argument List:', str(real_args))

    dry_run_flag = False

    # No arg call means Dry-Run
    if args_count > 0:
        arg_flag = real_args[0]
        # dry_run_flag = bool(arg_flag)   # only value 'true' will result in True
        dry_run_flag = False
    else:
        dry_run_flag = True

    cleaup_payers(dry_run_flag)

def cleaup_payers(is_dry_run):
    print('Is dry run? ', is_dry_run)

    # Use high-level Resource API, instead of boto3.client
    dynamodb_resource = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb_resource.Table('hds-dev-payerinfo-0.0.0')

    print('Connected to DynamoDB...')

    payers_to_keep = {'SPACE', 'PYR1', '33099', 'TED-D', 'LALIT'}

    attributes_to_get = ['trading_partner_id', 'payer_id', 'payer_name', 'active', 'created_by', 'created_on']
    projectionExp = ', '.join(attributes_to_get)    # convert to string

    deleted_payer_count = 0

    with open('output.csv', 'w', newline='') as output_csv:
        csv_writer = csv.DictWriter(output_csv, fieldnames=attributes_to_get)
        
        # Header for CSV
        csv_writer.writeheader()

        has_items = True
        last_key  = False

        while has_items:
            if last_key:
                data = table.scan(ExclusiveStartKey=last_key, ProjectionExpression=projectionExp)
            else:
                data = table.scan(ProjectionExpression=projectionExp)

            if 'LastEvaluatedKey' in data:
                has_items = True
                last_key  = data['LastEvaluatedKey']
            else:
                has_items = False
                last_key  = False
            
            items_in_page = data['Items']
            print(type(items_in_page), ' has total items ', len(items_in_page))

            # iterate items in page
            for item_idx in range(len(items_in_page)):
                item_as_dict = items_in_page[item_idx]
                payer_id    = item_as_dict['payer_id']
                payer_name  = item_as_dict['payer_name']

                if (payer_id not in payers_to_keep):           
                    csv_writer.writerow(rowdict=item_as_dict)
                    
                    # Delete from table, if real run
                    if False == is_dry_run:
                        try:
                            response = table.delete_item(
                                Key={
                                    'payer_id': payer_id
                                }
                            )
                        except ClientError as e:
                            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                                print(e.response['Error']['Message'])
                            else:
                                raise
                        else:
                            print("DeleteItem succeeded: ", json.dumps(response, indent=4))
                    
                    deleted_payer_count += 1

        if False == is_dry_run:
            print('Refer output.csv fo the Deleted records...Total count: ', deleted_payer_count)
        else:
            print('Refer output.csv fo the marked-for-deletion records...Total count: ', deleted_payer_count)

if __name__ == "__main__":
    main()