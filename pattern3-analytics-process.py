import traceback
import json
from elasticsearch import Elasticsearch
import sys
import requests

#thsi process is pattern 3 for takaful search - this pattern identifies medicines vs issues.
#COMMON

###################################################################################################
#M1
def  loadData4mElasticSearch():
	try:
		query=json.dumps({
		"size": 1000000,
		"fields" :  ["ITEM", "STATUS","PROVIDER","NETWORKTYPE","BENEFSTATUS","TREATMENTDATE","PROFESSIONAL","CLAIMEDAMOUNT","ITEMPAYERSHARE","CHRONIC","SPECASSESSMENT","CLAIMID"],
		"query": {
		"bool": {
		"must": [
		{"match": {"PROVIDERTYPE": "Pharmacy"}},
		{ "match": { "FOB":"Out-Patient" }}
		]} }
		})
		response = requests.post('http://localhost:9200/nas_details/nas_claim_details/_search?scroll=1m', data=query)
		
		jsonDoc = json.loads(response.text)
		counter=1
		for obj in jsonDoc['hits']['hits']:
			print obj
			print '----------------------------------'+str(counter)
			counter+=1
		#for
	#try
	except:
		traceback.print_exc()
	#except
	finally:
		print 'end of process...'
	#finally
#M1
###################################################################################################
if __name__=='__main__':
	if sys.argv[1]=='1':
		loadData4mElasticSearch()
	#if
#if