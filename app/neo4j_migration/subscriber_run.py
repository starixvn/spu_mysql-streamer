import sys
import datetime
import json
from neo4j_migration.model_bootstrap import Neo4jContext, DEFAULT_DATABASE
from neo4j_migration.nodes import Subscriber,NodeEncoder

class SubscriberRun(Neo4jContext):
    def __init__(self, label="Subscriber",database=DEFAULT_DATABASE):
        super().__init__(label, database)

    @classmethod
    def single_query(cls, tx, keyID:int):
        select_query = "MATCH (n:Subscriber {SubscriberID: $Subscriber}) \
        RETURN n.SubscriberID AS SubscriberID, n.SubscriberName as SubscriberName"

        result = tx.run(select_query, Subscriber=keyID)
        nodes = []
        nodes = [Subscriber(row["SubscriberID"]
                                 , row["SubscriberName"]) for row in result]

        if len(nodes) > 0:
            return nodes.pop()

        return None

    @classmethod
    def create_batch(cls, tx, items:list):
        raws = json.dumps(items, cls=NodeEncoder)
        batch = json.loads(raws)
        create_bulk = """
            UNWIND $batch as row
            MERGE (n:Subscriber {SubscriberID:row.SubscriberID})
            ON CREATE SET n.SubscriberID=row.SubscriberID
                , n.SubscriberName=row.SubscriberName
            ON MATCH SET n.SubscriberName=row.SubscriberName
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
            MERGE (n:Subscriber {SubscriberID:row.SubscriberID})
            ON MATCH SET n.SubscriberName=row.SubscriberName
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
            MATCH (n:Subscriber {SubscriberID:row.SubscriberID})
            DETACH DELETE n
            """

        tx.run(remove_all, batch=batch)

    #binlog processing
    @classmethod
    def create_binlog(cls, tx, raws:list):
        batch = [Subscriber(r["after"]["SubscriberID"],r["after"]["SubscriberName"]) for r in raws]
        cls.create_batch(tx, batch)

    @classmethod
    def update_binlog(cls, tx, raws:list):
        batch = [Subscriber(r["after"]["SubscriberID"],r["after"]["SubscriberName"]) for r in raws]
        cls.update_batch(tx, batch)

    @classmethod
    def delete_binlog(cls, tx, raws:list):
        batch = [Subscriber(r["before"]["SubscriberID"], None) for r in raws]
        cls.delete_batch(tx, batch)