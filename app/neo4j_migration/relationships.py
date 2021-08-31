import sys
from datetime import datetime

class BELONG_TO_SUB(object):
    def __init__(self, CTSCustID, subscriberID, createdDate=datetime.now().replace(microsecond=0).isoformat()):
        self.CTSCustID=CTSCustID
        self.SubscriberID=subscriberID
        if createdDate is not None:
            createdDate=str(createdDate).replace(" ", "T")
        self.CreatedDate=createdDate

class USED_DEVICE(object):
    def __init__(self, CTSCustID, deviceID, createdTime=datetime.now().replace(microsecond=0).isoformat()):
        self.CTSCustID=CTSCustID
        self.DeviceID=deviceID
        if createdTime is not None:
            createdTime=str(createdTime).replace(" ", "T")
        self.CreatedTime=createdTime

class HAS_CATEGORY(object):
    def __init__(self, CTSCustID, categoryID, createdDate=datetime.now().replace(microsecond=0).isoformat()):
        self.CTSCustID=CTSCustID
        self.CategoryID=categoryID
        if createdDate is not None:
            createdDate=str(createdDate).replace(" ", "T")
        self.CreatedDate=createdDate


class HAS_EVIDENCE(object):
    def __init__(self, CTSCustID, evidenceID, level=None,createdDate=datetime.now().replace(microsecond=0).isoformat()):
        self.CTSCustID=CTSCustID
        self.EvidenceID=evidenceID
        if createdDate is not None:
            createdDate=str(createdDate).replace(" ", "T")
        self.CreatedDate=createdDate
        self.Level=level

class HAS_STATUS(object):
    def __init__(self, CTSCustID, statusID, createdDate=datetime.now().replace(microsecond=0).isoformat()):
        self.CTSCustID=CTSCustID
        self.StatusID=statusID
        if createdDate is not None:
            createdDate=str(createdDate).replace(" ", "T")
        self.CreatedDate=createdDate
        
class ADD_MANUALLY(object):
    def __init__(self, fromCTSCustID, toCTSCustID, createdDate=datetime.now().replace(microsecond=0).isoformat()):
        self.FromCTSCustID=fromCTSCustID
        self.ToCTSCustID=toCTSCustID
        if createdDate is not None:
            createdDate=str(createdDate).replace(" ", "T")
        self.CreatedDate=createdDate

class HAS_EXCEPTION(object):
    def __init__(self, fromCTSCustID, toCTSCustID, createdDate=datetime.now().replace(microsecond=0).isoformat()):
        self.FromCTSCustID=fromCTSCustID
        self.ToCTSCustID=toCTSCustID
        if createdDate is not None:
            createdDate=str(createdDate).replace(" ", "T")
        self.CreatedDate=createdDate
