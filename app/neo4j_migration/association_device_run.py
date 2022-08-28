import sys
import datetime
import json
from neo4j_migration.model_bootstrap import Neo4jContext, DEFAULT_DATABASE
from neo4j_migration.nodes import Device,NodeEncoder

class AssociationByDeviceRun(Neo4jContext):
    def __init__(self, label="Device",database=DEFAULT_DATABASE):
        super().__init__(label, database)

    @classmethod
    def create_batch(cls, tx, items:list):
        raws = json.dumps(items, cls=NodeEncoder)
        batch = json.loads(raws)
        ################
        nIDs = None
        create_bulk = """
            UNWIND $batch as row
            MERGE (n:Device {DeviceID:toInteger(row.DeviceID)})
            ON CREATE SET n.CreatedDate=datetime(row.CreatedDate)
            ON MATCH SET n.LastAssDate=datetime(row.CreatedDate)
            RETURN ID(n) as NID
            """

        # node first
        n_result = tx.run(create_bulk,batch=batch)
        nIDs = [row["NID"] for row in n_result]

        # relationship
        rIDS=None
        used_device = """
            UNWIND $batch as row
            MATCH (c:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
            MATCH (n:Device {DeviceID:toInteger(row.DeviceID)})
            MERGE (c)-[r:USED_DEVICE]->(n)
            ON CREATE SET r.CreatedDate = datetime(row.CreatedDate)
            RETURN ID(r) AS RID
            """
        r_result = tx.run(used_device,batch=batch)
        rIDS = [row["RID"] for row in n_result]

        return nIDs

    @classmethod
    def update_batch(cls, tx, items:list):
        raws = json.dumps(items, cls=NodeEncoder)
        batch = json.loads(raws)

        # add relationship
        rIDs=None
        used_device = """
            UNWIND $batch as row
            MATCH (c:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
            MERGE (n:Device {DeviceID:toInteger(row.DeviceID)})
            MERGE (c)-[r:USED_DEVICE]-(n)
            ON CREATE SET r.CreatedDate=datetime(row.CreatedDate)
                ,n.CreatedDate=datetime(row.CreatedDate)
            ON MATCH SET r.CreatedDate=datetime(row.CreatedDate)
            RETURN ID(r) AS RID
            """
        r_result = tx.run(used_device,batch=batch)
        nIDs = [row["RID"] for row in r_result]

        return rIDs

    @classmethod
    def delete_batch(cls, tx, items:list):
        raws = json.dumps(items, cls=NodeEncoder)
        batch = json.loads(raws)
        # remove AIO
        remove_all = """
            UNWIND $batch as row
            MATCH (n:Device {DeviceID:toInteger(row.DeviceID)})
            MATCH (c:CTSCustomer {CTSCustID:toInteger(row.CTSCustID)})
            MATCH (c)-[r:USED_DEVICE]->(n)
            DELETE r
            WITH n
            OPTIONAL MATCH (n)-[r:USED_DEVICE]-(:CTSCustomer)
            WITH COUNT(r) AS Quantity, n
            WHERE Quantity = 0
            DELETE n
            """
        tx.run(remove_all,batch=batch)

    #binlog processing
    @classmethod
    def create_binlog(cls, tx, raws:list):
        batch = [Device(r["after"]["DCSDeviceID"],r["after"]["CTSCustID"],r["after"]["SubscriberID"],r["after"]["CreatedTime"]) for r in raws]
        cls.create_batch(tx, batch)

    @classmethod
    def update_binlog(cls, tx, raws:list):
        batch = [Device(r["after"]["DCSDeviceID"],r["after"]["CTSCustID"],r["after"]["SubscriberID"],r["after"]["CreatedTime"]) for r in raws]
        cls.update_batch(tx, batch)

    @classmethod
    def delete_binlog(cls, tx, raws:list):
        batch = [Device(r["before"]["DCSDeviceID"], r["before"]["CTSCustID"]) for r in raws]
        cls.delete_batch(tx, batch)
