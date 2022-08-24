import sys
import datetime
import json
from app.neo4j_migration.model_bootstrap import Neo4jContext, DEFAULT_DATABASE
from app.neo4j_migration.nodes import CTSCustomer,NodeEncoder
from app.neo4j_migration.relationships import ASS_BY_AI

class AssociationByAIRun(Neo4jContext):
    def __init__(self, label="ASS_BY_AI",database=DEFAULT_DATABASE):
        super().__init__(label, database)

    @classmethod
    def single_query(cls, tx, fromCustID:int, toCustID:int):
        select_query = """
                        MATCH (n:CTSCustomer {CustID: $FromCustID})-[r:ASS_BY_AI]-(c:CTSCustomer {CustID: $ToCustID})
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
                    MERGE (f:CTSCustomer {CustID:toInteger(row.FromCustID), CustSubID: 0})
                    MERGE (t:CTSCustomer {CustID:toInteger(row.ToCustID), CustSubID: 0})
                    MERGE (f)-[r:ASS_BY_AI]->(t)
                    ON CREATE SET r.CreatedDate=datetime(row.CreatedDate)
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
                    MATCH (f:CTSCustomer {CustID:toInteger(row.FromCustID), CustSubID: 0})
                    MATCH (t:CTSCustomer {CustID:toInteger(row.ToCustID), CustSubID: 0})
                    WHERE row.FromCustID <> row.ToCustID
                    MERGE (f)-[r:ASS_BY_AI]->(t)
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
                MATCH (f:CTSCustomer {CustID:toInteger(row.FromCustID), CustSubID: 0})
                MATCH (t:CTSCustomer {CustID:toInteger(row.ToCustID), CustSubID: 0})
                MATCH (f)-[r:ASS_BY_AI]->(t)
                DELETE r
               """
        tx.run(remove_all, batch=batch)

    #binlog processing
    @classmethod
    def create_binlog(cls, tx, raws:list):
        batch = [ASS_BY_AI(r["after"]["FromCustID"],r["after"]["ToCustID"],r["after"]["CreatedDate"]) for r in raws]
        cls.create_batch(tx, batch)

    @classmethod
    def update_binlog(cls, tx, raws:list):
        batch = [ASS_BY_AI(r["after"]["FromCustID"],r["after"]["ToCustID"],r["after"]["CreatedDate"]) for r in raws]
        cls.update_batch(tx, batch)

    @classmethod
    def delete_binlog(cls, tx, raws:list):
        batch = [ASS_BY_AI(r["before"]["FromCustID"],r["before"]["ToCustID"]) for r in raws]
        cls.delete_batch(tx, batch)
