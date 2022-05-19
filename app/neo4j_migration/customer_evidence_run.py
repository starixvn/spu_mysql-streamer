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
            MATCH (n:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
            MATCH (c:Evidence {EvidenceID:toInteger(row.EvidenceID)})
            WHERE row.Level = 0
            MERGE (n)-[r:HAS_EVIDENCE]->(c)
            ON CREATE SET r.CreatedDate=datetime(row.CreatedDate)
            WITH DISTINCT n,r
            SET n:FlaggedCustomer
            RETURN ID(r) as RID
            """
        
        # node first
        r_result = tx.run(create_bulk, batch=batch)
        rIDs = [row["RID"] for row in r_result]

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