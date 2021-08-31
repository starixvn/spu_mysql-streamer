import sys
import datetime
import json
from neo4j_migration.model_bootstrap import Neo4jContext, DEFAULT_DATABASE
from neo4j_migration.nodes import CTSCustomer,NodeEncoder

class CustomerRun(Neo4jContext):
    def __init__(self, label="CTSCustomer",database=DEFAULT_DATABASE):
        super().__init__(label, database)

    @classmethod
    def single_query(cls, tx, keyID:int):
        select_query = """
                MATCH (n:CTSCustomer {CTSCustID:$CTSCustID})
                RETURN n.CTSCustID AS CTSCustID, n.CustID as CustID, n.RegisterName as RegisterName, n.Username as Username,n.Username2 as Username2
            """

        result = tx.run(select_query, CTSCustID=keyID)
        nodes = []
        nodes = [CTSCustomer(row["CTSCustID"]
                                 , row["CustID"]
                                 , row["RegisterName"]
                                 , row["Username"]
                                 , row["Username2"]) for row in result]

        if len(nodes) > 0:
            return nodes.pop()

        return None

    @classmethod
    def create_batch(cls, tx, items:list):
        raws = json.dumps(items, cls=NodeEncoder)
        batch = json.loads(raws)
        #####################
        create_bulk = """
            UNWIND $batch as row
            MERGE (n:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
            ON CREATE SET n.CTSCustID=toInteger(row.CTSCustID)
                , n.CustID=toInteger(row.CustID)
                , n.RegisterName=row.RegisterName
                , n.Username=row.Username
                , n.Username2=row.Username2
                , n.CustSubID=row.CustSubID
            ON MATCH SET n.CustID=toInteger(row.CustID)
                , n.RegisterName=row.RegisterName
                , n.Username=row.Username
                , n.Username2=row.Username2
                , n.CustSubID=row.CustSubID
            RETURN ID(n) as NID
            """
        
        # node first
        n_result = tx.run(create_bulk, batch = batch)
        nIDs = [row["NID"] for row in n_result]

        # relationships
        #BELONG_TO_SUB
        rIDs=None
        belong_to_sub = """
            UNWIND $batch as row
            MATCH (n:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
            MATCH (s:Subscriber {SubscriberID:toInteger(row.SubscriberID)})
            WHERE row.SubscriberID > 0
            MERGE (n)-[r:BELONG_TO_SUB]-(s)
            ON CREATE SET r.CreatedDate=datetime(row.CreatedDate)
            RETURN ID(r) AS RID
            """
        r_result = tx.run(belong_to_sub, batch = batch)

        #HAS_STATUS
        has_status = """
            UNWIND $batch as row
            MERGE (n:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
            MERGE (s:StaticList {ItemID:row.CustStatusID, ListID: 1})
            MERGE (n)-[r:HAS_STATUS]-(s)
            ON CREATE SET r.CreatedDate=datetime(row.CreatedDate)
            RETURN ID(r) AS RID
            """
        r_result = tx.run(has_status, batch = batch)

        return nIDs

    @classmethod
    def update_batch(cls, tx, items:list):
        raws = json.dumps(items, cls=NodeEncoder)
        batch = json.loads(raws)
        #####################
        update_bulk = """
            UNWIND $batch as row
            MERGE (n:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
            ON MATCH SET n.CustID=row.CustID
                , n.RegisterName=row.RegisterName
                , n.Username=row.Username
                , n.Username2=row.Username2
                , n.CustSubID=row.CustSubID
            RETURN ID(n) as NID
            """
        
        # node first
        n_result = tx.run(update_bulk, batch = batch)
        nIDs = [row["NID"] for row in n_result]

        # remove relationship
        #BELONG_TO_SUB
        remove_belong_to_sub = """
            UNWIND $batch as row
            MATCH (n:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
            MATCH (n)-[r:BELONG_TO_SUB]-()
            DELETE r
            """
        tx.run(remove_belong_to_sub, batch = batch)

        #HAS_STATUS
        remove_has_status = """
            UNWIND $batch as row
            MATCH (n:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
            MATCH (n)-[r:HAS_STATUS]-()
            DELETE r
            """
        tx.run(remove_has_status, batch = batch)

        # add relationship
        belong_to_sub = """
            UNWIND $batch as row
            MATCH (n:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
            MATCH (s:Subscriber {SubscriberID:row.SubscriberID})
            MERGE (n)-[r:BELONG_TO_SUB]-(s)
            ON MATCH SET r.CreatedDate=datetime(row.CreatedDate)
            RETURN ID(r) AS RID
            """
        r_result = tx.run(belong_to_sub, batch = batch)

        #HAS_STATUS
        has_status = """
            UNWIND $batch as row
            MATCH (n:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
            MATCH (s:StaticList {ItemID:row.CustStatusID, ListID: 1})
            MERGE (n)-[r:HAS_STATUS]-(s)
            ON CREATE SET r.CreatedDate=datetime(row.CreatedDate)
            RETURN ID(r) AS RID
            """
        r_result = tx.run(has_status, batch = batch)

        return nIDs

    @classmethod
    def delete_batch(cls, tx, items:list):
        raws = json.dumps(items, cls=NodeEncoder)
        # remove AIO
        remove_all = """
            UNWIND $batch as row
            MATCH (n:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
            DETACH DELETE n
            """
        tx.run(remove_all, batch = json.loads(raws))

    #binlog processing
    @classmethod
    def create_binlog(cls, tx, raws:list):
        batch = [CTSCustomer(r["after"]["CTSCustID"],r["after"]["CustID"],r["after"]["RegisterName"],r["after"]["UserName"],r["after"]["UserName2"],r["after"]["SubscriberID"],r["after"]["CustSubID"],r["after"]["CustStatusID"],r["after"]["CreatedDate"]) for r in rows]
        cls.create_batch(tx, batch)

    @classmethod
    def update_binlog(cls, tx, raws:list):
        batch = [CTSCustomer(r["after"]["CTSCustID"],r["after"]["CustID"],r["after"]["RegisterName"],r["after"]["UserName"],r["after"]["UserName2"],r["after"]["SubscriberID"],r["after"]["CustSubID"],r["after"]["CustStatusID"],r["after"]["CreatedDate"]) for r in rows]
        cls.update_batch(tx, batch)

    @classmethod
    def delete_binlog(cls, tx, raws:list):
        batch = [CTSCustomer(r["before"]["CTSCustID"]) for r in rows]
        cls.delete_batch(tx, batch)
