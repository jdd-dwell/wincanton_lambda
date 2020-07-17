#
# Wincanton import lambda - email sql, no database
#
# Author: aw
# Dated: march 2020
#

import boto3
import xml.etree.ElementTree as ET
import os
import sys
import json
from base64 import b64decode
from botocore.exceptions import ClientError

from urllib import request, parse

########################################################################
# Main fucntion handler
########################################################################
def handle (event, context ):
    print('STARTING')

    dbuser = os.environ['dbuser'] 
    dbname = os.environ['dbname']

    source_bucket = event['Records'][0]['s3']['bucket']['name']
    file = event['Records'][0]['s3']['object']['key']

    if 'wincanton_live/' in file:
        dbpassword = os.environ['dbpasswordlive']
        hostname = os.environ['dbhostnamelive']
        print('Processing (live): ' + file)
    else:
        print('Processing: (dev)' + file)
        dbpassword = os.environ['dbpassword']
        hostname = os.environ['hostname']
    
    fileName, fileExt = os.path.splitext(file)
    if fileExt != '.xml':
        print('File is not XML' + file) 
        return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": ""
        }
    
    src = str(source_bucket) + '/' + str(file)
    processedFile =  os.path.dirname(file) + '/processed/'+ os.path.basename(file)

    s3 = boto3.resource('s3')
    s3c = boto3.client('s3')

    s3obj = s3c.get_object(Bucket=source_bucket, Key=file)
    object_content = s3obj[u'Body'].read().decode('-1252')

    root = ET.fromstring(object_content)
    for Order in root:
        ThirdPartyOrderCode = Order.attrib['ThirdPartyOrderCode']
        for OrderLine in Order.iter('OrderLine'):
                for OrderLineItem in OrderLine.iter('OrderLineItem'):
                  StatusCode = OrderLineItem.attrib['StatusCode']
                  StatusDesc = OrderLineItem.attrib['StatusDesc']                                     
    
    try:    
            
        if ThirdPartyOrderCode.startswith('C1-'):
            ThirdPartyOrderCode.find('-')
            collectionId = ThirdPartyOrderCode.split('-')
            CollectionIdEnd = collectionId[1]
            
            sql = "INSERT INTO `wincanton_log_table` (description,order_id,prefixed_order_id,in_out,call_type)  \
                VALUES ('%s','%s','%s','%s','%s')" % (src,ThirdPartyOrderCode,CollectionIdEnd,'i',StatusCode)
            ThirdPartyOrderCode = Order.attrib['ThirdPartyOrderCode']
            
            print(sql)
            
            sql = "UPDATE orders_deliveries SET wincanton_collection_stautus = '%s' WHERE order_id = %s" \
                     % \
                (StatusDesc,CollectionIdEnd)
        
            print(sql)
            
        elif ThirdPartyOrderCode.startswith('S-'):
            ThirdPartyOrderCode.find('-')
            ToshopID = ThirdPartyOrderCode.split('-')
            ToshopIdEnd = ToshopID[1]
            ThirdPartyOrderCode = ToshopIdEnd
            
            sql = "INSERT INTO `wincanton_log_table` (description,order_id,prefixed_order_id,in_out,call_type)  \
                VALUES ('%s','%s','%s','%s','%s')" % (src,ThirdPartyOrderCode,Order.attrib['ThirdPartyOrderCode'],'i',StatusCode)
            
            print(sql)
        else:
            ThirdPartyOrderCode = Order.attrib['ThirdPartyOrderCode']
            sql = "INSERT INTO `wincanton_log_table` (description,order_id,in_out,call_type)  \
            VALUES ('%s','%s','%s','%s')" % (src,ThirdPartyOrderCode,'i',StatusCode)

            print(sql)

        s3 = boto3.resource('s3')
        s3c = boto3.client('s3')

        s3obj = s3c.get_object(Bucket=source_bucket, Key=file)
        object_content = s3obj[u'Body'].read().decode('-1252')

        root = ET.fromstring(object_content)
        
        try:
            log_xml_to_db( file, root )

        except Exception as e:
            print (str(e))
        print(result)
    except Exception as e: 
        print (str(e))

#
# Write 
#
def log_xml_to_db( fileName, root ):

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
                            else:
                                if StatusCode == 'COMP-ROUT':
                                    StatusDesc = 'Delivery booked'

                try:

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

                    print(sql)
                    #
                    # update tables based on input file
                    #
                            
                    if ThirdPartyOrderCode.startswith('S-'):
                        ThirdPartyOrderCode.find('-')
                        ToshopID = ThirdPartyOrderCode.split('-')
                        ToshopIdEnd = ToshopID[1]
                        ThirdPartyOrderCode = ToshopIdEnd

                    sql = "UPDATE sohead SET ordStatusWincanton='%s', ordLastUpdatedWincanton='%s' WHERE id = %s" \
                        % \
                        (StatusDesc,StatusDate,ThirdPartyOrderCode)

                    print(sql)
                        
                    if StatusCode == 'COMP-ROUT':
                        DeliveryWindowText.find('-')
                        times = DeliveryWindowText.split(' - ')
                        startTime = times[0]
                        endTime   = times[1]
                        sql = "SELECT id FROM delslot WHERE sord = '%s' AND deldate = '%s'" % (ThirdPartyOrderCode, VisitDate)
                        print(sql)


                        if cursor.rowcount == 1 :
                            row = cursor.fetchone()
                            delslotID = row[0]
                            sql = "UPDATE delslot SET deldate='%s', route='%s', tripday=1, starthour='%s', endhour='%s' WHERE id ='%s'" \
                                    %  \
                                    (VisitDate, RouteNum, startTime, endTime, delslotID)
                            print(sql)
                        else:
                            tripday = 1
                        sql = "INSERT delslot (sord, deldate,vehicle, route, tripday, starthour, endhour, comm) VALUES('%s','%s', '%s', '%s', '%s', '%s', '%s','%s')" \
                                % \
                                (ThirdPartyOrderCode, VisitDate,'Wincanton',RouteNum, tripday, startTime, endTime, 'added from xml import')

                        print(sql)

                    sql = "SELECT id from delslot WHERE sord = %s " % ThirdPartyOrderCode 
                    
                    print(sql)
                     
                    delSlotId = 999999

                    Status = 'Delivered'
                    if StatusCode.startswith('FAIL'):
                        Status = 'Missed Delivery'
                    sql = "INSERT INTO orders_deliveries_confirmations (order_id, delslot_id, `date`, creator, `rating`, `status`, notes ) \
                            VALUES (%s, %s, NOW(), 'LAMBDA', 0, '%s', 'WINCANTON XML - delslot')" \
                            % \
                            (ThirdPartyOrderCode, delSlotId[0], Status)
                    print(sql)

                    Status = 'Delivered'
                    if StatusCode == 'COMP-DELS':
                        Status = 'Delivered'
                        sql = "INSERT INTO orders_deliveries_confirmations (order_id, delslot_id, `date`, creator, `rating`, `status`, notes ) \
                            VALUES (%s, %s, NOW(), 'LAMBDA', 0, '%s', 'WINCANTON XML - delslot')" \
                            % \
                            (ThirdPartyOrderCode,  delSlotId[0], Status)
                        print(sql)
                    
                        if StatusCode == 'COMP-ORDR':
                            sql = "UPDATE wincanton_create_order SET processed='%s',lastupdated='%s%'  WHERE id = %s" \
                                % \
                                ('Y',NOW,ThirdPartyOrderCode,DeliveryStatus)
                                
                    print(sql)
                    
                        
                except Exception as e:
                    print('cannot write xml to DB [' +str(e) +']')

