import sys
import datetime
import json
from neo4j_migration.model_bootstrap import Neo4jContext, DEFAULT_DATABASE
from neo4j_migration.nodes import NodeEncoder
from neo4j_migration.relationships import GROUP_BY_AI

class AssociationGroupByAIRun(Neo4jContext):
    def __init__(self, label="GROUP_BY_AI",database=DEFAULT_DATABASE):
        super().__init__(label, database)
        
    # batch query processing
    @classmethod
    def create_batch(cls, tx, items:list):
        raws=json.dumps(items, cls=NodeEncoder)
        batch=json.loads(raws)
        create_batch = """
                    UNWIND $batch as row
                    MERGE (c:CTSCustomer {CustID:toInteger(row.CustID), CustSubID: 0})
                    MERGE (g:AIGroup {GroupID:toInteger(row.NewGroupID)})
                    MERGE (c)-[r:GROUP_BY_AI]->(g)
                        ON CREATE SET r.CreatedDate=datetime(row.CreatedDate)
                            , r.OriginGroupID=row.NewOriginGroupID
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
                    MATCH (c:CTSCustomer {CustID:toInteger(row.CustID), CustSubID: 0})-[r:GROUP_BY_AI]->(g:AIGroup {GroupID:toInteger(row.OldGroupID)})
                    DELETE r
                    WITH c, row
                    MERGE (g:AIGroup {GroupID:toInteger(row.NewGroupID)})
                    MERGE (c)-[r:GROUP_BY_AI]->(g)
                        ON CREATE SET r.CreatedDate=datetime(row.CreatedDate)
                            , r.OriginGroupID=row.NewOriginGroupID
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
                MATCH (c:CTSCustomer {CustID:toInteger(row.CustID), CustSubID: 0})-[r:GROUP_BY_AI]->(g:AIGroup {GroupID:toInteger(row.OldGroupID)})
                DELETE r
               """
        tx.run(remove_all, batch=batch)

    #binlog processing
    @classmethod
    def create_binlog(cls, tx, raws:list):
        batch = [GROUP_BY_AI(r["after"]["CustID"],r["after"]["GroupID"],0,r["after"]["OriginGroupID"],0,r["after"]["CreatedDate"]) for r in raws]
        cls.create_batch(tx, batch)

    @classmethod
    def update_binlog(cls, tx, raws:list):
        batch = [GROUP_BY_AI(r["after"]["CustID"],r["after"]["GroupID"],r["before"]["GroupID"],r["after"]["OriginGroupID"],r["before"]["OriginGroupID"],r["after"]["CreatedDate"]) for r in raws]
        cls.update_batch(tx, batch)

    @classmethod
    def delete_binlog(cls, tx, raws:list):
        batch = [GROUP_BY_AI(r["before"]["CustID"],0,r["before"]["GroupID"],0,r["before"]["OriginGroupID"]) for r in raws]
        cls.delete_batch(tx, batch)