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
		file = open('/opt/takaful/processed-data/pattern3-input.csv','w')
		file.write("ITEM~STATUS~PROVIDER~NETWORKTYPE~BENEFSTATUS~TREATMENTDATE~PROFESSIONAL~CLAIMEDAMOUNT~ITEMPAYERSHARE~CHRONIC~SPECASSESSMENT~CLAIMID\n")
		for obj in jsonDoc['hits']['hits']:
			row="PH01~PH02~PH03~PH04~PH05~PH06~PH07~PH08~PH09~PH10~PH11~PH12"
			for key,val in  obj['fields'].iteritems():
				if  'ITEM'==key:
					row=row.replace('PH01',val[0])
				#if
				elif  'STATUS'==key:
					row=row.replace('PH02',val[0])
				#if
				elif  'PROVIDER'==key:
					row=row.replace('PH03',val[0])
				#if
				elif  'NETWORKTYPE'==key:
					row=row.replace('PH04',val[0])
				#if
				elif  'BENEFSTATUS'==key:
					row=row.replace('PH05',val[0])
				#if
				elif  'TREATMENTDATE'==key:
					rrow=row.replace('PH06',val[0])
				#if
				elif  'PROFESSIONAL'==key:
					row=row.replace('PH07',val[0])
				#if
				elif  'CLAIMEDAMOUNT'==key:
					row=row.replace('PH08',val[0])
				#if
				elif  'ITEMPAYERSHARE'==key:
					row=row.replace('PH09',val[0])
				#if
				elif  'CHRONIC'==key:
					row=row.replace('PH10',val[0])
				#if
				elif  'SPECASSESSMENT'==key:
					row=row.replace('PH11',val[0])
				#if
				elif  'CLAIMID'==key:
					row=row.replace('PH12',val[0])
				#if
			#for
			file.write(str(row.encode('utf-8').strip())+'\n')
		#for
		file.close()
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