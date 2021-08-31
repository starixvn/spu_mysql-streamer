import sys
import datetime
import json
from neo4j_migration.model_bootstrap import Neo4jContext, DEFAULT_DATABASE
from neo4j_migration.nodes import CTSCustomer, Evidence,NodeEncoder
from neo4j_migration.relationships import HAS_EVIDENCE

class CustEvidenceRun(Neo4jContext):
    def __init__(self, label="HAS_EVIDENCE",database=DEFAULT_DATABASE):
        super().__init__(label, database)

    @classmethod
    def single_query(cls, tx, CTSCustID:int, evidenceID:int):
        select_query = """
                MATCH (n:CTSCustomer {CTSCustID:$CTSCustID})-[r:HAS_EVIDENCE]-(c:Evidence {EvidenceID:$EvidenceID})
                RETURN r.CreatedDate as CreatedDate
                """

        result = tx.run(select_query, CTSCustID=CTSCustID, EvidenceID=evidenceID)
        nodes = []
        nodes = [row["CreatedDate"] for row in result]

        if len(nodes) > 0:
            return nodes.pop()

        return None

    @classmethod
    def create_batch(cls, tx, items:list):
        raws = json.dumps(items, cls=NodeEncoder)
        batch = json.loads(raws)
        create_bulk = """
            UNWIND $batch as row
            MATCH (n:CTSCustomer {CTSCustID:row.CTSCustID})
            MATCH (c:Evidence {EvidenceID:row.EvidenceID})
            WHERE row.Level = 0
            MERGE (n)-[r:HAS_EVIDENCE]-(c)
            ON CREATE SET r.CreatedDate=datetime(row.CreatedDate)
            WITH n,c,r
            SET n:FlaggedCustomer
            RETURN ID(r) as RID
            """
        
        # node first
        r_result = tx.run(create_bulk, batch=batch)
        rIDs = [row["RID"] for row in r_result]

        # merge SEEN_ACCOUNT if any
        seen_account = """
            UNWIND $batch as row
            MATCH (c1:FlaggedCustomer {CTSCustID:toInteger(row.CTSCustID)})
            MATCH (c1)-[r1:USED_DEVICE]->(n:Device)<-[r2:USED_DEVICE]-(c2:CTSCustomer)
            WHERE c1.CustID <> c2.CustID
            WITH c1,c2,MIN(CASE WHEN r1.CreatedDate > r2.CreatedDate THEN r1.CreatedDate ELSE r2.CreatedDate END) AS SeenDate
            MERGE (c1)-[r:SEEN_ACCOUNT]->(c2)
            ON CREATE SET r.FirstSeenDate = SeenDate
            ON MATCH SET r.FirstSeenDate = CASE WHEN r.FirstSeenDate > SeenDate THEN SeenDate ELSE r.FirstSeenDate END
            RETURN ID(r) AS RID
            """
        #r_result = tx.run(seen_account, batch=batch)

        # push seen_account
        push_seen_account = """
            UNWIND $batch as row
            MATCH (c:FlaggedCustomer {CTSCustID:toInteger(row.CTSCustID)})-[:USED_DEVICE]->(d:Device)
            MERGE (n:FlaggedSeenAccount {CTSCustID: c.CTSCustID, DeviceID: d.DeviceID})
            ON CREATE SET n.CreatedDate = date(),n.SeenStats=0
            RETURN ID(n) AS NID
            """
        r_result = tx.run(push_seen_account, batch=batch)

        return rIDs

    @classmethod
    def update_batch(cls, tx, items:list):
        raws = json.dumps(items, cls=NodeEncoder)
        batch = json.loads(raws)
        # add relationship
        create_bulk = """
            UNWIND $batch as row
            MATCH (n:CTSCustomer {CTSCustID:row.CTSCustID})
            MATCH (c:Evidence {EvidenceID:row.EvidenceID})
            WHERE row.Level = 0
            MERGE (n)-[r:HAS_EVIDENCE]-(c)
            ON MATCH SET r.CreatedDate=datetime(row.CreatedDate)
            RETURN ID(r) as RID
            """
        r_result = tx.run(create_bulk, batch=batch)
        rIDs = [row["RID"] for row in r_result]

        return rIDs

    @classmethod
    def delete_batch(cls, tx, items:list):
        raws = json.dumps(items, cls=NodeEncoder)
        batch = json.loads(raws)
        # remove AIO
        remove_all = """
            UNWIND $batch as row
            MATCH (n:CTSCustomer {CTSCustID:row.CTSCustID})
            MATCH (c:Evidence {EvidenceID:row.EvidenceID})
            MATCH (n)-[r:HAS_EVIDENCE]-(c)
            DELETE r
            WITH n
            WHERE NOT EXISTS((n)-[:HAS_EVIDENCE]-(:Evidence))
            REMOVE n:FlaggedCustomer
            RETURN COUNT(n)
            """
        tx.run(remove_all, batch=batch)

    #binlog processing
    @classmethod
    def create_binlog(cls, tx, raws:list):
        batch = [HAS_EVIDENCE(r["after"]["CTSCustID"],r["after"]["EvidenceID"],r["after"]["Level"],r["after"]["CreatedDate"]) for r in raws]
        cls.create_batch(tx, batch)

    @classmethod
    def update_binlog(cls, tx, raws:list):
        batch = [HAS_EVIDENCE(r["after"]["CTSCustID"],r["after"]["EvidenceID"],r["after"]["Level"],r["after"]["CreatedDate"]) for r in raws]
        cls.update_batch(tx, batch)

    @classmethod
    def delete_binlog(cls, tx, raws:list):
        batch = [HAS_EVIDENCE(r["before"]["CTSCustID"],r["before"]["EvidenceID"]) for r in raws]
        cls.delete_batch(tx, batch)
