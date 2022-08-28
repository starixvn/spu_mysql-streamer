import sys
import datetime
import json
from neo4j_migration.model_bootstrap import Neo4jContext, DEFAULT_DATABASE

class CTSCustomerRun(Neo4jContext):
    def __init__(self, label="CTSCustomer",database=DEFAULT_DATABASE):
        super().__init__(label, database)

    @classmethod
    def create_binlog(cls, tx, raws:list):
        nIDs = []
        batch = [r["after"] for r in raws]
        #####################
        create_bulk = """
            UNWIND $batch as row
            MERGE (n:CTSCustomer {CustID: toInteger(row.CustID), CustSubID: toInteger(row.CustSubID)})
            SET n.CTSCustID = toInteger(row.CTSCustID)
                , n.RegisterName = row.RegisterName
                , n.UserName = row.UserName
                , n.UserName2 = row.UserName2
            WITH n, toInteger(row.Recommend) AS recommendId, toInteger(row.IsInternal) AS isInternal, toInteger(row.IsLicensee) AS isLicensee
            CALL apoc.do.when(isInternal = 1 AND isInternal IS NOT NULL,
                'SET cus:IsInternal RETURN cus AS node', 'RETURN cus AS node',
                {cus : n}
            ) YIELD value
            WITH n, recommendId, isLicensee
            CALL apoc.do.when(isLicensee = 1 AND isLicensee IS NOT NULL,
                'SET cus:IsLicensee RETURN cus AS node', 'RETURN cus AS node',
                {cus : n}
            ) YIELD value
            WITH n, recommendId
            WHERE n.CustID <> recommendId AND recommendId > 0
            MERGE (agent:CTSCustomer {CustID: recommendId, CustSubID: 0})
            WITH n, agent
            MERGE (n)<-[:HAS_DOWNLINE]-(agent)
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
            MERGE (n)-[r:BELONG_TO_SUB]->(s)
            ON CREATE SET r.CreatedDate = datetime(row.CreatedDate)
            RETURN ID(r) AS RID
            """
        r_result = tx.run(belong_to_sub, batch = batch)

        #HAS_STATUS
        has_status = """
            UNWIND $batch as row
            MATCH (n:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
            MATCH (s:StaticList {ItemID:row.CustStatusID, ListID: 1})
            MERGE (n)-[r:HAS_STATUS]->(s)
            ON CREATE SET r.CreatedDate=datetime(row.CreatedDate)
            RETURN ID(r) AS RID
            """
        r_result = tx.run(has_status, batch = batch)

        return nIDs

    @classmethod
    def update_binlog(cls, tx, raws:list):
        # affected list
        raws_updates = [raw for raw in raws if raw["after"]["SubscriberID"] != raw["before"]["SubscriberID"] or raw["after"]["CustStatusID"] != raw["before"]["CustStatusID"] or raw["after"]["UserName"] != raw["before"]["UserName"] or raw["after"]["UserName2"] != raw["before"]["UserName2"] or raw["after"]["RegisterName"] != raw["before"]["RegisterName"] or raw["after"]["IsInternal"] != raw["before"]["IsInternal"] or raw["after"]["IsLicensee"] != raw["before"]["IsLicensee"]]
        if len(raws_updates) == 0:
            return 0
        

        #Update IsInternal
        #Update IsLicensee
        update_lic_internal_tag = """
            UNWIND $batch as row
            MATCH (n:CTSCustomer {CTSCustID:toInteger(row.after.CTSCustID)})
            REMOVE n:IsInternal
            REMOVE n:IsLicensee
            WITH n, toInteger(row.after.IsInternal) AS isInternal, toInteger(row.after.IsLicensee) AS isLicensee
            CALL apoc.do.when(isInternal = 1 AND isInternal IS NOT NULL,
                'SET cus:IsInternal RETURN cus AS node', 'RETURN cus AS node',
                {cus : n}
            ) YIELD value
            WITH n, isLicensee
            CALL apoc.do.when(isLicensee = 1 AND isLicensee IS NOT NULL,
                'SET cus:IsLicensee RETURN cus AS node', 'RETURN cus AS node',
                {cus : n}
            ) YIELD value
            RETURN ID(n) as NID
            """
        respone = tx.run(update_lic_internal_tag, batch = raws_updates)


        # 1st remove 
        removeIDs =  []
        #BELONG_TO_SUB
        remove_belong_to_sub = """
            UNWIND $batch as row
            MATCH (n:CTSCustomer {CTSCustID:toInteger(row.after.CTSCustID)})
            MATCH (n)-[r:BELONG_TO_SUB]-(s:Subscriber)
            WHERE toInteger(row.after.SubscriberID) <> toInteger(row.before.SubscriberID) OR NOT EXISTS(row.after.SubscriberID)
            DELETE r
            RETURN ID(r) AS RID
            """
        respone = tx.run(remove_belong_to_sub, batch = raws_updates)
        removeIDs = [row["RID"] for row in respone]

        #HAS_STATUS
        remove_has_status = """
            UNWIND $batch as row
            MATCH (n:CTSCustomer {CTSCustID:toInteger(row.after.CTSCustID)})
            MATCH (n)-[r:HAS_STATUS]-(s:StaticList)
            WHERE toInteger(row.after.CustStatusID) <> toInteger(row.before.CustStatusID) OR NOT EXISTS(row.after.CustStatusID)
            DELETE r
            RETURN ID(r) AS RID
            """
        respone = tx.run(remove_has_status, batch = raws_updates)
        removeIDs = removeIDs + [row["RID"] for row in respone]

        # 2nd update
        nIDs = []
        after = [r["after"] for r in raws_updates]
        #####################
        update_bulk = """
            UNWIND $batch as row
            MATCH (n:CTSCustomer {CustID: toInteger(row.CustID), CustSubID: toInteger(row.CustSubID)})
            SET n.CTSCustID = toInteger(row.CTSCustID)
                , n.RegisterName = row.RegisterName
                , n.UserName = row.UserName
                , n.UserName2 = row.UserName2
            RETURN ID(n) as NID
            """
        
        # node first
        n_result = tx.run(update_bulk, batch = after)
        nIDs = [row["NID"] for row in n_result]

        # add relationship
        if len(removeIDs)>0:
            belong_to_sub = """
                UNWIND $batch as row
                MATCH (n:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
                MATCH (s:Subscriber {SubscriberID:row.SubscriberID})
                WHERE row.SubscriberID > 0
                MERGE (n)-[r:BELONG_TO_SUB]-(s)
                ON CREATE SET r.CreatedDate=datetime(row.CreatedDate)
                RETURN ID(r) AS RID
                """
            r_result = tx.run(belong_to_sub, batch = after)

            #HAS_STATUS
            has_status = """
                UNWIND $batch as row
                MATCH (n:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
                MATCH (s:StaticList {ItemID:row.CustStatusID, ListID: 1})
                WHERE row.CustStatusID > 0
                MERGE (n)-[r:HAS_STATUS]-(s)
                ON CREATE SET r.CreatedDate=datetime(row.CreatedDate)
                RETURN ID(r) AS RID
                """
            r_result = tx.run(has_status, batch = after)

        return nIDs

    @classmethod
    def delete_binlog(cls, tx, raws:list):
        before = [r["before"] for r in raws]
        # remove AIO
        remove_all = """
            UNWIND $batch as row
            MATCH (n:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
            //SET n:ArchivedCustomer
            DETACH DELETE n
            """
        tx.run(remove_all, batch = before)
