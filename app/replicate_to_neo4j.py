import sys
from neo4j_migration.ctscustomer_run import CTSCustomerRun
from neo4j_migration.subscriber_run import SubscriberRun
from neo4j_migration.category_run import CustomerCategoryRun
from neo4j_migration.evidence_run import EvidenceRun
from neo4j_migration.association_device_run import AssociationByDeviceRun
from neo4j_migration.association_manual_run import AssociationByManualRun
from neo4j_migration.association_remove_run import AssociationRemoveRun
from neo4j_migration.customer_classification_run import CTSCustomerClassificationRun
from neo4j_migration.staticlist_run import StaticListRun
from neo4j_migration.customer_evidence_run import CustEvidenceRun
#from neo4j_migration.nodes import CTSCustomer,Subscriber,Category,Evidence,Device,CustomerCategory,StaticList
#from neo4j_migration.relationships import ADD_MANUALLY,HAS_EVIDENCE,HAS_EXCEPTION
from replication import BuildPayload

class Neo4jMessageMapper(object):
    def __init__(self, payload:BuildPayload):
        self.op = payload.message.op
        self.schema = payload.message.schema
        self.table = payload.message.table
        self.rows = payload.message.rows

    def sync(self):
        sync_rs = None
        sync_subfix = str(self.table).lower()
        try:
            runner = globals()["%sRun" % self.table]()
            runner_call = getattr(runner, 'sync_%s' % self.op)
            sync_rs = runner_call(self.rows)

        except Exception as ex:
            sys.stderr.write('Unhandle exception ({0} - Neo4jMessageMapper mapping failed: {1}): try again\n'.format(self.table, str(ex)))

        return sync_rs