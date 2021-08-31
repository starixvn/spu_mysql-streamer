import sys
from datetime import datetime
import json
sys.path.append("C:\\Users\\bobby.nguyen\\source\\repos\\CustomerTrackingSystem-DB\\dbscript\\CTS_GraphDB\\MySQLStreamer\\app\\")
from customer_run import CustomerRun
from subscriber_run import SubscriberRun
from category_run import CategoryRun
from evidence_run import EvidenceRun
from association_device_run import AssociationDeviceRun
from customer_classification_run import CustomerClassificationRun
from association_manual_run import AssociationManualRun
from staticlist_run import StaticListRun
from customer_evidence_run import CustomerEvidenceRun
from nodes import CTSCustomer,Subscriber,Category,Evidence,Device,CustomerCategory,StaticList,NodeEncoder
from relationships import ADD_MANUALLY,HAS_EVIDENCE

def main():
    print("Hello World!")

    rows = [
		{"op": "c", "after": {"CTSAssDevID": 40203637, "CTSCustID": 40397440, "DCSDeviceID": 28877896, "SubscriberID": 101, "CreatedTime": "2021-04-08 11:40:01", "InsertTime": "2021-04-08 11:40:00"}}
		, {"op": "c", "after": {"CTSAssDevID": 40203638, "CTSCustID": 44186426, "DCSDeviceID": 27668530, "SubscriberID": 168, "CreatedTime": "2021-04-08 11:40:03", "InsertTime": "2021-04-08 11:40:00"}}
		, {"op": "c", "after": {"CTSAssDevID": 40203639, "CTSCustID": 49915302, "DCSDeviceID": 28693431, "SubscriberID": 168, "CreatedTime": "2021-04-08 11:40:05", "InsertTime": "2021-04-08 11:40:00"}}
		, {"op": "c", "after": {"CTSAssDevID": 40203640, "CTSCustID": 50678363, "DCSDeviceID": 24096461, "SubscriberID": 168, "CreatedTime": "2021-04-08 11:40:00", "InsertTime": "2021-04-08 11:40:00"}}
		, {"op": "c", "after": {"CTSAssDevID": 40203641, "CTSCustID": 50683526, "DCSDeviceID": 28877894, "SubscriberID": 146, "CreatedTime": "2021-04-08 11:40:00", "InsertTime": "2021-04-08 11:40:00"}}
		, {"op": "c", "after": {"CTSAssDevID": 40203642, "CTSCustID": 50682201, "DCSDeviceID": 8161250, "SubscriberID": 169, "CreatedTime": "2021-04-08 11:40:01", "InsertTime": "2021-04-08 11:40:00"}}
		, {"op": "c", "after": {"CTSAssDevID": 40203643, "CTSCustID": 50683519, "DCSDeviceID": 28877895, "SubscriberID": 1297, "CreatedTime": "2021-04-08 11:40:01", "InsertTime": "2021-04-08 11:40:00"}}
		, {"op": "c", "after": {"CTSAssDevID": 40203644, "CTSCustID": 2414631, "DCSDeviceID": 28877897, "SubscriberID": 112, "CreatedTime": "2021-04-08 11:40:03", "InsertTime": "2021-04-08 11:40:00"}}
		, {"op": "c", "after": {"CTSAssDevID": 40203645, "CTSCustID": 50591457, "DCSDeviceID": 28827890, "SubscriberID": 168, "CreatedTime": "2021-04-08 11:40:05", "InsertTime": "2021-04-08 11:40:00"}}
	]

    ids = [r["after"]["CTSCustID"] for r in rows]
    print(ids);

    cust = CustomerRun()
    c1 = CTSCustomer(100,100,"register_100","username_100","username2_100", 100, None, 11)
    c2 = CTSCustomer(101,101,"register_101","username_101","username2_101", 1, None, 11)
    customers = [c1,c2]
    write = cust.update(customers)
    print(write)
    read:CTSCustomer = cust.read(101)
    print(read.RegisterName)

    ##############
    sub = SubscriberRun()
    s1 = Subscriber(1, "Subscriber_11")
    s2 = Subscriber(102, "Subscriber_102_d")
    subs = [s1,s2]
    sub.update(subs)

    #############
    cate = CategoryRun()
    c1 = Category(1, "Category_1c")
    c2 = Category(2, "Category_2g")
    cates = [c1,c2]
    cate.update(cates)


    #############
    evi = EvidenceRun()
    e1 = Evidence(1, "Evidence_1", "Code_1")
    e2 = Evidence(2, "Evidence_2", "Code_2u")
    evis = [e1,e2]
    evi.update(evis)


    #############
    dev = AssociationDeviceRun()
    d1 = Device(1, 100)
    d2 = Device(2, 101)
    d3 = Device(1, 101)
    d3 = Device(2, 100)
    devs = [d1,d2,d3]
    dev.update(devs)

    #############
    cc = CustomerClassificationRun()
    cc1 = CustomerCategory(CTSCustID=100, categoryID=1, beforeCategoryID=2)
    cc2 = CustomerCategory(101, 1)
    ccs = [cc1,cc2]

    #json_data=[json.dumps(item, cls=NodeEncoder) for item in ccs]
    #json_data=json.dumps(ccs, cls=NodeEncoder)
    #print(json_data)
    cc.update(ccs)

    #############
    manual=AssociationManualRun()
    ad1 = ADD_MANUALLY(100,101)
    ad2 = ADD_MANUALLY(100,1)
    ads=[ad1,ad2]

    manual.create(ads)

    #ListID, ItemID, ItemNameDisplay, Status
    static=StaticListRun()
    st1 = StaticList(1,11,"ItemNameDisplay_1","Status_1")
    st2 = StaticList(100,110,"ItemNameDisplay_100","Status_100")
    sts=[st1,st2]
    static.create(sts)


     #############
    erun=CustomerEvidenceRun()
    er1 = HAS_EVIDENCE(100,1,0)
    er2 = HAS_EVIDENCE(1,2,0)
    ers=[er1,er2]

    erun.create(ers)

if __name__ == "__main__":
    main()
