import sys
from neo4j import GraphDatabase
#sys.path.append("/app/")
#from configuration import util_get_config

class QueryStats:
    label = None
    total = 0
    def __init__(self, label, total):
        self.label = label
        self.total = int(total)

class Neo4jCollector:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_user(self, name):
        with self.driver.session() as session:
            user = session.write_transaction(self._create_and_return_user, name)
            print('----> Neo4j collector')
            print(user)

    def get_node_stats(self):
        with self.driver.session() as session:
            result = session.run("MATCH (n) "
                        "RETURN distinct(labels(n))[0] as label, count(1) as total")
            #print('----> Neo4j collector - get_node_stats')
            node_stats = []
            for record in result:
                #print(record.values())
                label, total = record.values()
                stat = QueryStats(label, total)
                node_stats.append(stat)

            #print(node_stats[0])
            return node_stats

    def get_relationship_stats(self):
        with self.driver.session() as session:
            result = session.run("MATCH ()-[r]->() "
                        "RETURN distinct(type(r)) as label, count(1) as total")
            #print('----> Neo4j collector - get_relationship_stats')
            node_stats = []
            for record in result:
                #print(record.values())
                label, total = record.values()
                stat = QueryStats(label, total)
                node_stats.append(stat)

            #print(node_stats[0])
            return node_stats

    @staticmethod
    def _create_and_return_user(tx, name):
        result = tx.run("CREATE (a:User) "
                        "SET a.name = $name "
                        "RETURN a.name + ', from node ' + id(a)", name=name)
        return result.single()[0]


#if __name__ == "__main__":
#    conf = util_get_config('neo4j')
#    print(dict(conf))
#    collector = Neo4jCollector(conf['uri'], conf['user'], conf['passwd'])
#    #collector.create_user("hello-world")
#    collector.get_node_stats()
#    collector.get_relationship_stats()
#    collector.close()