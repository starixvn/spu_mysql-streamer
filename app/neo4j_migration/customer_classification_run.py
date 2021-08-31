import sys
import datetime
import json
from neo4j_migration.model_bootstrap import Neo4jContext, DEFAULT_DATABASE
from neo4j_migration.nodes import CustomerCategory,NodeEncoder
from neo4j_migration.relationships import HAS_CATEGORY

class CTSCustomerClassificationRun(Neo4jContext):
    def __init__(self, label="HAS_CATEGORY",database=DEFAULT_DATABASE):
        super().__init__(label, database)

    @classmethod
    def single_query(cls, tx, CTSCustID:int, categoryID:int):
        select_query = """
                MATCH (n:CTSCustomer {CTSCustID:$CTSCustID})-[r:HAS_CATEGORY]-(c:CustomerCategory {CategoryID:$CategoryID})
                RETURN n.Username AS Username, c.CategoryName AS CategoryName, r.CreatedDate as CreatedDate
                """

        result = tx.run(select_query, CTSCustID=CTSCustID, CategoryID=categoryID)
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
            MERGE (n:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
            MERGE (c:CustomerCategory {CategoryID:row.CategoryID})
            MERGE (n)-[r:HAS_CATEGORY]-(c)
            ON CREATE SET r.CreatedDate=datetime(row.CreatedDate)
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
        # remove relationship
        remove_relationship = """
            UNWIND $batch as row
            MATCH (n:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
            MATCH (c:CustomerCategory {CategoryID:row.BeforeCategoryID})
            WHERE row.BeforeCategoryID <> row.CategoryID OR NOT EXISTS(row.CategoryID)
            MATCH (n)-[r:HAS_CATEGORY]-(c)
            DELETE r
            """
        tx.run(remove_relationship,batch=batch)

        # add relationship
        create_bulk = """
            UNWIND $batch as row
            MATCH (n:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
            MATCH (c:CustomerCategory {CategoryID:row.CategoryID})
            WHERE row.BeforeCategoryID <> row.CategoryID
            MERGE (n)-[r:HAS_CATEGORY]-(c)
            ON CREATE SET r.CreatedDate=datetime(row.CreatedDate)
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
            MATCH (n:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
            MATCH (c:CustomerCategory {CategoryID:row.CategoryID})
            MATCH (n)-[r:HAS_CATEGORY]-(c)
            DELETE r
            """
        tx.run(remove_all, batch=batch)

    #binlog processing
    @classmethod
    def create_binlog(cls, tx, raws:list):
        batch = [CustomerCategory(r["after"]["CTSCustID"],r["after"]["CategoryID"],r["after"]["CreatedDate"]) for r in raws]
        cls.create_batch(tx, batch)

    @classmethod
    def update_binlog(cls, tx, raws:list):
        batch = [CustomerCategory(r["after"]["CTSCustID"],r["after"]["CategoryID"],r["after"]["CreatedDate"],r["before"]["CategoryID"]) for r in raws]
        cls.update_batch(tx, batch)

    @classmethod
    def delete_binlog(cls, tx, raws:list):
        batch = [CustomerCategory(r["before"]["CTSCustID"],r["before"]["CategoryID"]) for r in raws]
        cls.delete_batch(tx, batch)
