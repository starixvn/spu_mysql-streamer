import sys
import datetime
import json
from neo4j_migration.model_bootstrap import Neo4jContext, DEFAULT_DATABASE
from neo4j_migration.nodes import CTSCustomer,NodeEncoder
from neo4j_migration.relationships import ASS_BY_IP

class AssociationByIPRun(Neo4jContext):
    def __init__(self, label="ASS_BY_IP",database=DEFAULT_DATABASE):
        super().__init__(label, database)

    @classmethod
    def single_query(cls, tx, fromCustID:int, toCustID:int):
        select_query = """
                        MATCH (n:CTSCustomer {CustID: $FromCustID})-[r:ASS_BY_IP]-(c:CTSCustomer {CustID: $ToCustID})
                        RETURN r.CreatedDate as CreatedDate
                        """

        result = tx.run(select_query, FromCustID=fromCustID, ToCustID=toCustID)
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
                    MATCH (f:CTSCustomer {CustID:toInteger(row.FromCustID)})
                    MATCH (t:CTSCustomer {CustID:toInteger(row.ToCustID)})
                    WHERE row.FromCustID <> row.ToCustID
                    MERGE (f)-[r:ASS_BY_IP]-(t)
                    ON CREATE set r.CreatedDate=datetime(row.CreatedDate)
                    RETURN ID(r) as NID
                """
        
        # node first
        r_result = tx.run(create_batch, batch=batch)

        rIDs = [row["NID"] for row in r_result]

        return rIDs

    @classmethod
    def update_batch(cls, tx, items:list):
        raws = json.dumps(items, cls=NodeEncoder)
        batch=json.loads(raws)
        create_one = """
                    UNWIND $batch as row
                    MATCH (f:CTSCustomer {CustID:toInteger(row.FromCustID)})
                    MATCH (t:CTSCustomer {CustID:toInteger(row.ToCustID)})
                    WHERE row.FromCustID <> row.ToCustID
                    MERGE (f)-[r:ASS_BY_IP]-(t)
                    ON MATCH SET r.CreatedDate=datetime(row.CreatedDate)
                    RETURN ID(r) as NID
                """
        
        # node first
        r_result = tx.run(create_one, batch=batch)
        rIDs = [row["NID"] for row in r_result]

        return rIDs

    @classmethod
    def delete_batch(cls, tx, items:list):
        # remove AIO
        raws = json.dumps(items, cls=NodeEncoder)
        batch=json.loads(raws)
        remove_all = """
                UNWIND $batch as row
                MATCH (f:CTSCustomer {CustID:toInteger(row.FromCustID)})
                MATCH (t:CTSCustomer {CustID:toInteger(row.ToCustID)})
                MERGE (f)-[r:ASS_BY_IP]-(t)
                DELETE r
               """
        tx.run(remove_all, batch=batch)

    #binlog processing
    @classmethod
    def create_binlog(cls, tx, raws:list):
        batch = [ASS_BY_IP(r["after"]["FromCustID"],r["after"]["ToCustID"],r["after"]["CreatedDate"]) for r in raws]
        cls.create_batch(tx, batch)

    @classmethod
    def update_binlog(cls, tx, raws:list):
        batch = [ASS_BY_IP(r["after"]["FromCustID"],r["after"]["ToCustID"],r["after"]["CreatedDate"]) for r in raws]
        cls.update_batch(tx, batch)

    @classmethod
    def delete_binlog(cls, tx, raws:list):
        batch = [ASS_BY_IP(r["before"]["FromCustID"],r["before"]["ToCustID"]) for r in raws]
        cls.delete_batch(tx, batch)
