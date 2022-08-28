import sys
import datetime
import json
from neo4j_migration.model_bootstrap import Neo4jContext, DEFAULT_DATABASE
from neo4j_migration.nodes import Evidence,NodeEncoder

class EvidenceRun(Neo4jContext):
    def __init__(self, label="Evidence",database=DEFAULT_DATABASE):
        super().__init__(label, database)

    @classmethod
    def single_query(cls, tx, keyID:int):
        select_query = """
            MATCH (n:Evidence {EvidenceID: $Evidence})
            RETURN n.EvidenceID AS EvidenceID, n.EvidenceName as EvidenceName,n.EvidenceCode AS EvidenceCode
            """

        result = tx.run(select_query, Evidence=keyID)
        nodes = []
        nodes = [Evidence(row["EvidenceID"]
                          , row["EvidenceName"]
                          , row["EvidenceCode"]) for row in result]

        if len(nodes) > 0:
            return nodes.pop()

        return None

    @classmethod
    def create_batch(cls, tx, items:list):
        raws = json.dumps(items, cls=NodeEncoder)
        batch = json.loads(raws)
        create_bulk = """
            UNWIND $batch as row
            MERGE (n:Evidence {EvidenceID:row.EvidenceID})
            ON CREATE SET n.EvidenceID=row.EvidenceID
                , n.EvidenceName=row.EvidenceName
                , n.EvidenceCode=row.EvidenceCode
            ON MATCH SET n.EvidenceName=row.EvidenceName
                , n.EvidenceCode=row.EvidenceCode
            RETURN ID(n) as NID
            """
        
        # node first
        n_result = tx.run(create_bulk, batch=batch)
        nIDs = [row["NID"] for row in n_result]

        return nIDs

    @classmethod
    def update_batch(cls, tx, items:list):
        raws = json.dumps(items, cls=NodeEncoder)
        batch = json.loads(raws)
        update_bulk = """
            UNWIND $batch as row
            MERGE (n:Evidence {EvidenceID:row.EvidenceID})
            ON MATCH SET n.EvidenceName=row.EvidenceName
                , n.EvidenceCode=row.EvidenceCode
            RETURN ID(n) as NID
            """
        
        # node first
        n_result = tx.run(update_bulk, batch=batch)
        nIDs = [row["NID"] for row in n_result]

        return nIDs

    @classmethod
    def delete_batch(cls, tx, items:list):
        raws = json.dumps(items, cls=NodeEncoder)
        batch = json.loads(raws)
        # remove AIO
        remove_all = """
            UNWIND $batch as row
            MATCH (n:Evidence {EvidenceID:row.EvidenceID})
            DETACH DELETE n
            """
        tx.run(remove_all, batch=batch)
    
    #binlog processing
    @classmethod
    def create_binlog(cls, tx, raws:list):
        batch = [Evidence(r["after"]["EvidenceID"],r["after"]["EvidenceName"],r["after"]["EvidenceCode"]) for r in raws]
        cls.create_batch(tx, batch)

    @classmethod
    def update_binlog(cls, tx, raws:list):
        batch = [Evidence(r["after"]["EvidenceID"],r["after"]["EvidenceName"],r["after"]["EvidenceCode"]) for r in raws]
        cls.update_batch(tx, batch)

    @classmethod
    def delete_binlog(cls, tx, raws:list):
        batch = [Evidence(r["before"]["EvidenceID"], None, None) for r in raws]
        cls.delete_batch(tx, batch)