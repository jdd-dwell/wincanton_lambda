import pymysql.cursors
import boto3
import xml.etree.ElementTree as ET
import os


dbuser = os.environ['dbuser']
dbname = os.environ['dbname']
dbpassword = os.environ['dbpassword']
hostname = os.environ['hostname']
             
# Connect to the database
def handle (event, context ):
    
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    file = event['Records'][0]['s3']['object']['key']
    src = str(source_bucket) + '/' + str(file)
    
    fileType = str(file).split('_')[-1]
    functionName = fileType.split('.')[0]


    connection = pymysql.connect(host = hostname,
                             user = dbuser,
                             db = dbname,
                             password = dbpassword
                             )
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "INSERT INTO `wincanton_log_table` (description)  \
                      VALUES (%s)"
            cursor.execute(sql, source_bucket + '/' + file)
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
            switch(functionName)(connection, str(file), root)
            
            if not src.find('-TEST.xml'):
                s3.Object(source_bucket, file + '. processed').copy_from(CopySource=src)
                
        except Exception as e:
            print (str(e))
        print(result)
    except Exception as e: 
        print (str(e))
    finally:
        connection.close()

#
#
#
def log_xml_to_db( connection, fileName, root):
    
    if root.attrib['Version'] != "4.0":
        raise Exception("Invalid version number [" + str(root.attrib['Version']) + ']')
        
    for Order in root:
        #print(Order.tag, Order.attrib)
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
                StatusChanged = OrderLineItem.attrib['StatusChanged']
                VisitNum = OrderLineItem.attrib['VisitNum']
                ActionType = OrderLineItem.attrib['ActionType']
                ConsignmentRef = OrderLineItem.attrib['ConsignmentRef']
                PackageNum = OrderLineItem.attrib['PackageNum']
                PackageTotal = OrderLineItem.attrib['PackageTotal']
                SuppChainLocationCode = OrderLineItem.attrib['SuppChainLocationCode']
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
                        print(sql)
                        cursor.execute(sql)
                        connection.commit() 
                        result = cursor.fetchone()
                except Exception as e:
                    print('cannot write xml to DB [' +str(e) +']')
#
# Catch all for process switcher, incase the process name cannot
# be matched to an existing funxtion
def invalid_process():
    raise Exception('Invalid file name')
   
#
# Cerate a switch staement
#
def switch(case):
    return {
        "COMP-DATE":process_comp_date,
        "COMP-LOAD":process_comp_load,
        "COMP-ORDR":process_comp_order,
        "COMP-PICK":process_comp_pick,
        "COMP-ROUT":process_comp_route,
        "COMP-STKR":process_comp_stkr,
    }.get(case, invalid_process)

#
# Process the XML from a COMP_DATE file (completion date)
# Status update to the order file
#
def process_comp_date( connect, filename, root):
    log_xml_to_db( connection, fileName, root )

    return True

#
# Process the XML from a COMP_LOAD file (XXXXXXXXXXXX)
#
def process_comp_load( connect, filename, root):
    log_xml_to_db( connection, fileName, root )

    return True    

#
# Process the XML from a COMP-ORDR file (complete order)
# This file type is to inform dwell that wincanton have
# received the order 
#
def process_comp_order( connection, fileName, root ):
    log_xml_to_db( connection, fileName, root )

    return True
    
#
# Process the XML from a COMP_PICK file (XXXXXXXXXXXX)
#
def process_comp_pick( connect, filename, root):
    log_xml_to_db( connection, fileName, root )

    return True   
    
#
# Process the XML from a COMP_ROUT file (XXXXXXXXXXXX)
#
def process_comp_route( connect, filename, root):
    log_xml_to_db( connection, fileName, root )

    return True
    
#
# Process the XML from a COMP_STKR file (XXXXXXXXXXXX)
#
def process_comp_stkr( connect, filename, root):
    log_xml_to_db( connection, fileName, root )

    return True    




