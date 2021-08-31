import sys
import datetime
import json
from neo4j_migration.model_bootstrap import Neo4jContext, DEFAULT_DATABASE
from neo4j_migration.nodes import CTSCustomer,NodeEncoder
from neo4j_migration.relationships import HAS_EXCEPTION

class AssociationRemoveRun(Neo4jContext):
    def __init__(self, label="HAS_EXCEPTION",database=DEFAULT_DATABASE):
        super().__init__(label, database)

    @classmethod
    def single_query(cls, tx, fromCTSCustID:int, toCTSCustID:int):
        select_query = """
                        MATCH (n:CTSCustomer {CTSCustID: $FromCTSCustID})-[r:HAS_EXCEPTION]-(c:CTSCustomer {CTSCustID: $ToCTSCustID})
                        RETURN r.CreatedDate as CreatedDate
                        """

        result = tx.run(select_query, FromCTSCustID=fromCTSCustID, ToCTSCustID=toCTSCustID)
        nodes = []
        nodes = [row["CreatedDate"] for row in result]

        if len(nodes) > 0:
            return nodes.pop()

        return None
        
    # batch query processing
    @classmethod
    def create_batch(cls, tx, items:list):
        raws=json.dumps(items, cls=NodeEncoder)
        batch=json.loads(raws)
        create_batch = """
                    UNWIND $batch as row
                    MATCH (f:CTSCustomer {CTSCustID:toInteger(row.FromCTSCustID)})
                    MATCH (t:CTSCustomer {CTSCustID:toInteger(row.ToCTSCustID)})
                    MERGE (f)-[r:HAS_EXCEPTION]-(t)
                    ON CREATE set r.CreatedDate=datetime(row.CreatedDate)
                    RETURN ID(r) as RID
                """
        
        # node first
        r_result = tx.run(create_batch, batch=batch)

        rIDs = [row["RID"] for row in r_result]

        return rIDs

    @classmethod
    def update_batch(cls, tx, items:list):
        raws = json.dumps(items, cls=NodeEncoder)
        batch=json.loads(raws)
        create_one = """
                    UNWIND $batch as row
                    MATCH (f:CTSCustomer {CTSCustID:toInteger(row.FromCTSCustID)})
                    MATCH (t:CTSCustomer {CTSCustID:toInteger(row.ToCTSCustID)})
                    MERGE (f)-[r:HAS_EXCEPTION]-(t)
                    ON MATCH SET r.CreatedDate=datetime(row.CreatedDate)
                    RETURN ID(r) as RID
                """
        
        # node first
        r_result = tx.run(create_one, batch=batch)
        rIDs = [row["RID"] for row in r_result]

        return rIDs

    @classmethod
    def delete_batch(cls, tx, items:list):
        # remove AIO
        raws = json.dumps(items, cls=NodeEncoder)
        batch=json.loads(raws)
        remove_all = """
                UNWIND $batch as row
                MATCH (f:CTSCustomer {CTSCustID:toInteger(row.FromCTSCustID)})
                MATCH (t:CTSCustomer {CTSCustID:toInteger(row.ToCTSCustID)})
                MERGE (f)-[r:HAS_EXCEPTION]-(t)
                DELETE r
               """
        tx.run(remove_all, batch=batch)

    #binlog processing
    @classmethod
    def create_binlog(cls, tx, raws:list):
        batch = [HAS_EXCEPTION(r["after"]["FromCTSCustID"],r["after"]["ToCTSCustID"],r["after"]["CreatedDate"]) for r in raws]
        cls.create_batch(tx, batch)

    @classmethod
    def update_binlog(cls, tx, raws:list):
        batch = [HAS_EXCEPTION(r["after"]["FromCTSCustID"],r["after"]["ToCTSCustID"],r["after"]["CreatedDate"]) for r in raws]
        cls.update_batch(tx, batch)

    @classmethod
    def delete_binlog(cls, tx, raws:list):
        batch = [HAS_EXCEPTION(r["before"]["FromCTSCustID"],r["before"]["ToCTSCustID"]) for r in raws]
        cls.delete_batch(tx, batch)
