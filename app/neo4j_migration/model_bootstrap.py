import sys
from neo4j import GraphDatabase
from configuration import util_get_config
from app_const import DEFAULT_DATABASE

class Neo4jConnector:
    def __init__(self, uri, user, password, database=None):
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database
        
        if uri is None or user is None or password is None:
            conf = util_get_config("neo4j")
            self.uri = conf["uri"]
            self.user = conf["user"]
            self.password = conf["passwd"]

        if self.database is None:
            self.database = DEFAULT_DATABASE

        if "database" in conf:
            self.database = conf["database"]

        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def close(self):
        self.driver.close();

class Neo4jContext(Neo4jConnector):
    def __init__(self, label, database):
        self.label = label
        self.database = database
        super().__init__(None, None, None, database)
    
    #single processing
    @classmethod
    def single_query(cls, tx, keyID:int):
        pass

    @classmethod
    def create_query(cls, tx, node):
        pass

    @classmethod
    def update_query(cls, tx, node):
        pass

    @classmethod
    def delete_query(cls, tx, node):
        pass

    #batch processing
    @classmethod
    def create_batch(cls, tx, nodes:list):
        pass

    @classmethod
    def update_batch(cls, tx, nodes:list):
        pass

    @classmethod
    def delete_batch(cls, tx, nodes:list):
        pass

    #binlog processing
    @classmethod
    def create_binlog(cls, tx, raws:list):
        pass

    @classmethod
    def update_binlog(cls, tx, raws:list):
        pass

    @classmethod
    def delete_binlog(cls, tx, raws:list):
        pass

    # object methods
    def read(self, keyID:int):
        result = None
        with self.driver.session(database=self.database) as session:
            result = session.read_transaction(self.single_query, keyID)
        return result

    def create_one(self, node):
        #print("create_one {0}.{2}.".format(self.database,self.label))
        result = None
        with self.driver.session(database=self.database) as session:
            session.write_transaction(self.create_query, node)
        return result

    def update_one(self, node):
        result = None
        with self.driver.session(database=self.database) as session:
            session.write_transaction(self.update_query, node)
        return result

    
    def delete_one(self, node):
        result = None
        with self.driver.session(database=self.database) as session:
            session.write_transaction(self.delete_query, node)
        return result

    # batch processing
    def create(self, nodes:list):
        result = None
        with self.driver.session(database=self.database) as session:
            session.write_transaction(self.create_batch, nodes)
        return result

    def update(self, nodes:list):
        result = None
        with self.driver.session(database=self.database) as session:
            session.write_transaction(self.update_batch, nodes)
        return result

    
    def delete(self, nodes:list):
        result = None
        with self.driver.session(database=self.database) as session:
            session.write_transaction(self.delete_batch, nodes)
        return result

    # raw processing
    def sync_c(self, raws:list):
        result = None
        with self.driver.session(database=self.database) as session:
            session.write_transaction(self.create_binlog, raws)
        return result

    def sync_u(self, raws:list):
        result = None
        with self.driver.session(database=self.database) as session:
            session.write_transaction(self.update_binlog, raws)
        return result

    
    def sync_d(self, raws:list):
        result = None
        with self.driver.session(database=self.database) as session:
            session.write_transaction(self.delete_binlog, raws)
        return result
