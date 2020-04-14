#
# Wincanton import lambda 
#
# Author: aw
# Dated: march 2020
#

import pymysql.cursors
import boto3
import xml.etree.ElementTree as ET
import os
import json
from botocore.exceptions import ClientError

from urllib import request, parse

#secrets_manager = boto3.client('secretsmanager')
#rds_credentials = json.loads(
#    secrets_manager.get_secret_value(SecretId='rds-credentials')['SecretString']
#)
#dbuser = rds_credentials['username']
#dbpassword = rds_credentials['password']

dbuser = os.environ['dbuser']
dbname = os.environ['dbname']
dbpassword = os.environ['dbpassword']
hostname = os.environ['hostname']

SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/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'
SLACK_CHANNEL = 'wincanton_report_exceptions'
SLACK_USER = 'HAL'

########################################################################
# Main fucntion handler
########################################################################
def handle (event, context ):

    source_bucket = event['Records'][0]['s3']['bucket']['name']
    file = event['Records'][0]['s3']['object']['key']

    src = str(source_bucket) + '/' + str(file)
    processedFile =  os.path.dirname(file) + '/processed/'+ os.path.basename(file)
    #src = source_bucket + "/" + file

    s3 = boto3.resource('s3')
    s3c = boto3.client('s3')

    s3obj = s3c.get_object(Bucket=source_bucket, Key=file)
    object_content = s3obj[u'Body'].read().decode('-1252')
    print(file)

    root = ET.fromstring(object_content)
    for Order in root:
        ThirdPartyOrderCode = Order.attrib['ThirdPartyOrderCode']
        for OrderLine in Order.iter('OrderLine'):
                for OrderLineItem in OrderLine.iter('OrderLineItem'):
                  StatusCode = OrderLineItem.attrib['StatusCode']

    connection = pymysql.connect(host = hostname,
                             user = dbuser,
                             db = dbname,
                             password = dbpassword
                             )
    try:
        with connection.cursor() as cursor:
            # Create a new record

            sql = "INSERT INTO `wincanton_log_table` (description,order_id,in_out,call_type)  \
                     VALUES ('%s','%s','%s','%s')" % (src,ThirdPartyOrderCode,'i',StatusCode)

            cursor.execute(sql)
            result = cursor.fetchone()

            # connection is not autocommit by default. So you must commit to save
            # your changes.
            connection.commit()

            s3 = boto3.resource('s3')
            s3c = boto3.client('s3')

            s3obj = s3c.get_object(Bucket=source_bucket, Key=file)
            object_content = s3obj[u'Body'].read().decode('-1252')

            root = ET.fromstring(object_content)

            try:
                log_xml_to_db( connection, file, root )

                s3.Object(source_bucket, processedFile).copy_from(CopySource=src)
                s3.Object(source_bucket, file).delete()

            except Exception as e:
                print (str(e))
            print(result)
    except Exception as e: 
        print (str(e))
    finally:
        connection.close()

#
# Write 
#
def log_xml_to_db( connection, fileName, root ):

    if root.attrib['Version'] != "4.0":
        raise Exception("Invalid version number [" + str(root.attrib['Version']) + ']')

    for Order in root:
        ThirdPartyCustCode = Order.attrib['ThirdPartyCustCode']
        ThirdPartyOrderCode = Order.attrib['ThirdPartyOrderCode']
        NumOfLines = Order.attrib['NumOfLines']
        NumOfItems = Order.attrib['NumOfItems']
        for OrderLine in Order.iter('OrderLine'):
            ThirdPartyOrderLineNum = OrderLine.attrib['ThirdPartyOrderLineNum']
            ThirdPartyOrderLineQty = OrderLine.attrib['ThirdPartyOrderLineQty']
            Manufacturer = OrderLine.attrib['Manufacturer']
            for OrderLineItem in OrderLine.iter('OrderLineItem'):
                OrderLineSeqNum = OrderLineItem.attrib['OrderLineSeqNum']
                StatusCode = OrderLineItem.attrib['StatusCode']
                StatusDesc = OrderLineItem.attrib['StatusDesc']
                LocationCode = OrderLineItem.attrib['LocationCode']
                Warehouse = OrderLineItem.attrib['Warehouse']
                VisitDate = OrderLineItem.attrib['VisitDate']
                DelOuCode = OrderLineItem.attrib['DelOuCode']
                RouteNum = OrderLineItem.attrib['RouteNum']
                DropNum = OrderLineItem.attrib['DropNum']
                DropTime = OrderLineItem.attrib['DropTime']
                StatusDate = OrderLineItem.attrib['StatusDate']
                StatusChanged = OrderLineItem.attrib['StatusChanged']
                DeliveryWindowText = OrderLineItem.attrib['DeliveryWindowText']
                VisitNum = OrderLineItem.attrib['VisitNum']
                ActionType = OrderLineItem.attrib['ActionType']
                RouteDetailsNum = OrderLineItem.attrib['RouteDetailsNum']
                ThirdPartyRouteCode = OrderLineItem.attrib['ThirdPartyRouteCode']
                CarrierRef = OrderLineItem.attrib['CarrierRef']
                #StatusChanged = OrderLineItem.attrib['StatusChanged']
                #VisitNum = OrderLineItem.attrib['VisitNum']
                #ActionType = OrderLineItem.attrib['ActionType']
                ConsignmentRef = OrderLineItem.attrib['ConsignmentRef']
                PackageNum = OrderLineItem.attrib['PackageNum']
                PackageTotal = OrderLineItem.attrib['PackageTotal']
                SuppChainLocationCode = OrderLineItem.attrib['SuppChainLocationCode']

                if StatusCode == 'COMP-STKR':
                    StatusDesc = 'Collected from DC'
                else:
                    if StatusCode == 'COMP-LOAD':
                        StatusDesc = 'Out for Delivery'
                    else:
                        if StatusCode == 'COMP-DELS':
                            StatusDesc = 'Delivered'
                        else:
                            if StatusCode == 'COMP-COLS':
                                StatusDesc = 'Collected'

                try:
                    with connection.cursor() as cursor:
                        sql = "INSERT INTO `wincanton_report_xml` \
                                    (srcName, cust_id, order_id, NumOfLines, NumOfItems, ThirdPartyOrderLineNum, \
                                     ThirdPartyOrderLineQty, Manufacturer, OrderLineSeqNum, StatusCode, StatusDesc, \
                                     LocationCode, Warehouse, VisitDate, DelOuCode, RouteNum, DropNum, DropTime, \
                                     StatusDate, StatusChanged, DeliveryWindowText, VisitNum, RouteDetailsNum, \
                                     ThirdPartyRouteCode, CarrierRef, ConsignmentRef, PackageNum, PackageTotal, \
                                     SuppChainLocationCode)  \
                                VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" \
                                % (fileName, ThirdPartyCustCode, ThirdPartyOrderCode, NumOfLines, NumOfItems, ThirdPartyOrderLineNum, \
                                   ThirdPartyOrderLineQty, Manufacturer, OrderLineSeqNum, StatusCode, StatusDesc, \
                                   LocationCode, Warehouse, VisitDate, DelOuCode, RouteNum, DropNum, DropTime, StatusDate, \
                                   StatusChanged, DeliveryWindowText, VisitNum, RouteDetailsNum, ThirdPartyRouteCode, CarrierRef, \
                                   ConsignmentRef, PackageNum, PackageTotal, SuppChainLocationCode)

                        cursor.execute(sql)
                        connection.commit()
                        result = cursor.fetchone()
                    #
                    # update tables based on input file
                    #

                        with connection.cursor() as cursor:
                            sql = "UPDATE sohead SET ordStatusWincanton='%s', ordLastUpdatedWincanton='%s' WHERE id = %s" \
                                       % \
                                        (StatusDesc,StatusDate,ThirdPartyOrderCode)

                            cursor.execute(sql)
                            print(sql)
                            if DeliveryWindowText.find('-') :
                                times = DeliveryWindowText.split(' - ')
                                startTime = times[0]
                                endTime   = times[1]

                                sql = "SELECT id FROM delslot WHERE sord = '%s' AND deldate = '%s'" % (ThirdPartyOrderCode, VisitDate)
                                cursor.execute(sql)
                                connection.commit()

                                if cursor.rowcount == 1 :
                                    row = cursor.fetchone()
                                    delslotID = row[0]
                                    sql = "UPDATE delslot SET deldate='%s', route='%s', tripday=1, starthour='%s', endhour='%s' WHERE id ='%s'" \
                                            %  \
                                            (VisitDate, RouteNum, startTime, endTime, delslotID)
                                else:
                                    tripday = 1
                                sql = "INSERT delslot (sord, deldate,vehicle, route, tripday, starthour, endhour, comm) VALUES('%s','%s', '%s', '%s', '%s', '%s', '%s','%s')" \
                                        % \
                                        (ThirdPartyOrderCode, VisitDate,'Wincanton',RouteNum, tripday, startTime, endTime, 'added from xml import')

                            cursor.execute(sql)
                            connection.commit()

                            sql = "SELECT id from delslot WHERE sord = %s " % ThirdPartyOrderCode
                            print(sql)
                            cursor.execute(sql)
                            connection.commit() 
                            delSlotId = cursor.fetchone()
                            print(delSlotId)

                            status = 'Delivered'
                            if StatusCode.startswith('FAIL'):
                                status = 'Missed Delivery'

                            sql = "INSERT INTO orders_deliveries_confirmations (order_id, delslot_id, `date`, creator, `rating`, `status`, notes ) \
                                    VALUES (%s, %s, NOW(), 'LAMBDA', 0, '%s', 'WINCANTON XML - delslot')" \
                                    % \
                                    (ThirdPartyOrderCode, delSlotId, status)

                            print(sql)
                            cursor.execute(sql)
                            connection.commit() 

                except Exception as e:
                    print('cannot write xml to DB [' +str(e) +']')

###############################################################################

def slack_message( message ):
    try:
        post = {'text': "{0}".format(message),
                'channel': SLACK_CHANNEL,
                'username': SLACK_USER,
                'icon_emoji': ':robot_face:'
        }

        req = request.post(SLACK_WEBHOOK_URL,
                            data=json.dumps(post),
                            headers={'Content-Type': 'application/json'}
                            ) 

        print('Response: ' + str(req.text))
        print('Response code: ' + str(req.status_code))
    except Exception as e:
        print('Exception: ' + str(e))

###
def get_secret(secret_name):


    region_name = "eu-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()

    client = session.client( service_name='secretsmanager', region_name=region_name )

    try:
        get_secret_value_response = client.get_secret_value( SecretId=secret_name )

    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])

    return json.loads(secret)
