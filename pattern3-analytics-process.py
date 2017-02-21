import traceback
import json
from elasticsearch import Elasticsearch
import sys
import requests
import pandas as pd
import spacy
import re

#thsi process is pattern 3 for takaful search - this pattern identifies medicines vs issues.
#COMMON
nlp = spacy.load('en')
###################################################################################################
#M2
def translateItems():
	try:
		
	#try
	except:
		traceback.print_exc()
	#except
#M2
###################################################################################################
#M1
def  loadNas_Details_Data4mElasticSearch():
	try:
		query=json.dumps({
		"size": 1000000,
		"fields" :  ["ITEM", "STATUS","PROVIDER","NETWORKTYPE","BENEFICIARY","TREATMENTDATE","PROFESSIONAL","ITEMPAYERSHARE","CHRONIC","SPECASSESSMENT","SERVICE","MASTERCONTRACT","CLAIMTYPE"],
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
		file.write("ITEM~STATUS~PROVIDER~NETWORKTYPE~BENEFICIARY~TREATMENTDATE~PROFESSIONAL~ITEMPAYERSHARE~CHRONIC~SPECASSESSMENT~SERVICE~MASTERCONTRACT~CLAIMTYPE\n")
		for obj in jsonDoc['hits']['hits']:
			row="PH01~PH02~PH03~PH04~PH05~PH06~PH07~PH08~PH09~PH10~PH11~PH12~PH13"
			for key,val in  obj['fields'].iteritems():
				if  'ITEMPAYERSHARE'==key:
					row=row.replace('PH08',val[0])
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
				elif  'BENEFICIARY'==key:
					row=row.replace('PH05',val[0])
				#if
				elif  'TREATMENTDATE'==key:
					row=row.replace('PH06',val[0])
				#if
				elif  'PROFESSIONAL'==key:
					row=row.replace('PH07',val[0])
				#if
				elif  'ITEM'==key:
					row=row.replace('PH01',val[0])
				#if
				elif  'CHRONIC'==key:
					row=row.replace('PH09',val[0])
				#if
				elif  'SPECASSESSMENT'==key:
					row=row.replace('PH10',val[0])
				#if
				elif  'SERVICE'==key:
					row=row.replace('PH11',val[0])
				#if
				elif  'MASTERCONTRACT'==key:
					row=row.replace('PH12',val[0])
				#if
				elif  'CLAIMTYPE'==key:
					row=row.replace('PH13',val[0])
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
#M2
def normalizeNasData():
	try:
		disease_lst = [line.strip().lower() for line in open('/opt/takaful/processed-data/concerned-disease-list.csv','r')]		
		inputDF = pd.read_csv('/opt/takaful/processed-data/pattern3-input.csv',delimiter='~')
		
		def vlidateSpecAssesment(val):
			try:
				lst =[disease_lst[item].lower() for item in range(len(disease_lst)) if disease_lst[item].lower() in val]
				if len(lst)==0:
					return 'NONE'
				#if
				else:
					return str(lst[0])
				#else
			#try
			except:
				traceback.print_exc()
			#except
		#def
		inputDF['SPECASSESSMENT'] = inputDF.apply(lambda row : vlidateSpecAssesment(row['SPECASSESSMENT'].lower()),axis=1)		
		inputDF = inputDF.drop(inputDF[inputDF['SPECASSESSMENT'].str.contains('NONE')].index)
		
		def vlidateItems(val):
			try:
				doc = nlp(val.decode('unicode-escape'))
				lst =[]
				for ent in doc.ents:
					if ent.label_=='ORG' and str(ent)!='MG' and str(ent) !='TUBE' and str(ent)!='IU' and str(ent)!='DOSE' and str(ent)!='SACHET' and str(ent)!='STRIP' and str(ent)!='ORAL' and str(ent)!='MG 3\'S' and str(ent)!='BOTTLE' and str(ent)!='UNIT' and str(ent)!='MCG' and str(ent)!='VIAL' and str(ent) !='MG 30\'S':
						#print ent,ent.label_
						lst.append(str(ent))
					#if	
				#for
				returnvar =  " ".join(lst) if len(lst)>=1 else "NONE"
				if returnvar=='NONE':
					index = re.search('\d',val)
					if index:
						returnvar = val[:index.start()].strip()
					#if
					elif '(' in val:
						returnvar = val[:val.index('(')].strip()
					#else
				#if
				
				return returnvar
			#try
			except:
				traceback.print_exc()
			#except
		#def
		inputDF['ITEM'] = inputDF.apply(lambda row : vlidateItems(row['ITEM']),axis=1)		
		#print inputDF.shape
		inputDF.to_csv('/opt/takaful/processed-data/pattern3-normalized.csv',header=True,index=False,index_label=False)

	#try
	except:
		traceback.print_exc()
	#except
	finally:
		print 'end of process...'
	#finally
#M2
###################################################################################################
if __name__=='__main__':
	if sys.argv[1]=='1':
		loadNas_Details_Data4mElasticSearch()
	#if
	if sys.argv[1]=='2':
		normalizeNasData()
	#if
#if