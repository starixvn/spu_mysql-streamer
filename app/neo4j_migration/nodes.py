import sys
from datetime import datetime
from neo4j_migration.relationships import BELONG_TO_SUB
from json import JSONEncoder

class BaseNode(object):
    def __init__(self, *args, **kwargs):
        self.timestamp=datetime.now().timestamp()

class NodeEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__

class CTSCustomer(BaseNode):
    def __init__(self, CTSCustID, custID=None, registerName=None, username=None, username2=None, subscriberID=None,custSubID=None,custStatusID=None,createdDate=datetime.now().replace(microsecond=0).isoformat()):
        self.CTSCustID=CTSCustID
        self.CustID=custID
        self.RegisterName=registerName
        self.Username=username
        self.Username2=username2
        self.SubscriberID=subscriberID
        self.CustSubID=custSubID
        if createdDate is not None:
            createdDate=str(createdDate).replace(" ", "T")
        self.CreatedDate=createdDate
        self.CustStatusID=custStatusID

    def get_subscriber(self):
        return BELONG_TO_SUB(self.CTSCustID,self.SubscriberID,self.CreatedDate)

class Device(BaseNode):
    def __init__(self, deviceID, CTSCustID=None,subscriberID=None,createdDate=datetime.now().replace(microsecond=0).isoformat()):
        self.DeviceID=deviceID
        self.CTSCustID=CTSCustID
        if createdDate is not None:
            createdDate=str(createdDate).replace(" ", "T")
        self.CreatedDate=createdDate
        self.SubscriberID=subscriberID

class Subscriber(BaseNode):
    def __init__(self, subscriberID, subscriberName):
        self.SubscriberID=subscriberID
        self.SubscriberName=subscriberName

class Category(BaseNode):
    def __init__(self, categoryID, categoryName:None):
        self.CategoryID=categoryID
        self.CategoryName=categoryName

class Evidence(BaseNode):
    def __init__(self, evidenceID, evidenceName, evidenceCode):
        self.EvidenceID=evidenceID
        self.EvidenceName=evidenceName
        self.EvidenceCode=evidenceCode

class StaticList(BaseNode):
    def __init__(self, listID, itemID, itemNameDisplay=None, status=None):
        self.ListID=listID
        self.ItemID=itemID
        self.ItemNameDisplay=itemNameDisplay
        self.Status=status

class CustomerCategory(BaseNode):
    def __init__(self, CTSCustID,categoryID,createdDate=datetime.now().replace(microsecond=0).isoformat(),beforeCategoryID=None):
        self.CTSCustID=CTSCustID
        self.CategoryID=categoryID
        if createdDate is not None:
            createdDate=str(createdDate).replace(" ", "T")
        self.CreatedDate=createdDate
        self.BeforeCategoryID=beforeCategoryID

