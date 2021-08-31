import sys
from datetime import datetime
import json
sys.path.append("C:\\Users\\bobby.nguyen\\source\\repos\\CustomerTrackingSystem-DB\\dbscript\\CTS_GraphDB\\MySQLStreamer\\app")
sys.path.append("C:\\Users\\bobby.nguyen\\source\\repos\\CustomerTrackingSystem-DB\\dbscript\\CTS_GraphDB\\MySQLStreamer\\app\\neo4j_migration")
from replicate_to_neo4j import Neo4jMessageMapper
def main():
    print("Hello World!")
    message = {"message": {"op": "c", "table": "AssociationByDevice", "schema": "CTS_DataCenter"}
			   , "rows": [
						{"op": "c", "after": {"CTSAssDevID": 40203637, "CTSCustID": 40397440, "DCSDeviceID": 28877896, "SubscriberID": 101, "CreatedTime": "2021-04-08 11:40:01", "InsertTime": "2021-04-08 11:40:00"}}
						, {"op": "c", "after": {"CTSAssDevID": 40203638, "CTSCustID": 44186426, "DCSDeviceID": 27668530, "SubscriberID": 168, "CreatedTime": "2021-04-08 11:40:03", "InsertTime": "2021-04-08 11:40:00"}}						
					]}
    
    strDate=datetime.fromisoformat("2021-04-01 00:01:01".replace(" ","T"))
    print(type(strDate), strDate.isoformat())
    strNow = datetime.now().isoformat()
    print(type(strNow))
    print(strDate.replace(microsecond=0).isoformat())
	#ids = [r["after"]["CTSCustID"] for r in message["rows"]]
    #print(ids)
	#
    #

    mapper=Neo4jMessageMapper(message)
    mapper.sync()

if __name__ == "__main__":
    main()
