import sys
from app.neo4j_migration.ctscustomer_run import CTSCustomerRun
from app.neo4j_migration.subscriber_run import SubscriberRun
from app.neo4j_migration.category_run import CustomerCategoryRun
from app.neo4j_migration.evidence_run import EvidenceRun
from app.neo4j_migration.association_device_run import AssociationByDeviceRun
from app.neo4j_migration.association_manual_run import AssociationByManualRun
from app.neo4j_migration.association_remove_run import AssociationRemoveRun
from app.neo4j_migration.association_by_AI_run import AssociationByAIRun
from app.neo4j_migration.association_group_by_AI_run import AssociationGroupByAIRun
from app.neo4j_migration.association_by_IP_run import AssociationByIPRun
from app.neo4j_migration.customer_classification_run import CTSCustomerClassificationRun
from app.neo4j_migration.staticlist_run import StaticListRun
from app.neo4j_migration.customer_evidence_run import CustEvidenceRun
from app.replication import BuildPayload

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
            raise Exception('Unhandle exception ({0} - Neo4jMessageMapper mapping failed: {1}): try again\n'.format(self.table, str(ex)))

        return sync_rs