import traceback
import json
from elasticsearch import Elasticsearch
import sys
import requests
import pandas as pd
import spacy
import re
import numpy as np

#thsi process is pattern 3 for takaful search - this pattern identifies medicines vs issues.

#grep pattern3-nextcare-report1.csv -e 'PHARMACY AND VACCINATIONS'
#COMMON
nlp = spacy.load('en')
ICD_REGEX = re.compile(r'.*([A-Z]\d+[\.]?\d*)-.*')
SPEC_REGEX = re.compile(r'([A-Z]\d+[\.]?\d*)\s+.*')
NEXT_REGEX = re.compile(r'(\d+[\.]?\d*)\s+.*')

def standardizeICD(val):
	try:
		regxLst = []
		for tmp in val.split(')'):
			match = re.findall(ICD_REGEX,tmp)
			if match:
				regxLst.append( match[0])
			#if
		#for	
		regxLst.sort()
		return  "~".join(regxLst)
	#try
	except:
		traceback.print_exc()
	#except
#def

def standardizeSpecAssesment(val):
	try:
		regxLst = []
		for tmp in val.split(','):
			match = re.findall(SPEC_REGEX,tmp)
			if match:
				regxLst.append( match[0])
			#if
		#for	
		regxLst.sort()
		return  "~".join(regxLst)
	#try
	except:
		traceback.print_exc()
	#except
#def

def standardizeSpec4NextCare(val):
	try:
		regxLst = []
		for tmp in val.split(','):
			match = re.findall(NEXT_REGEX,tmp)
			if match:
				regxLst.append( match[0])
			#if
		#for	
		regxLst.sort()
		return  "~".join(regxLst)
	#try
	except:
		traceback.print_exc()
	#except
#def
###################################################################################################
#M1
def  loadNas_Details_Data4mElasticSearch():
	try:
		query=json.dumps({
		"size": 9000000,
		"fields" :  ["PROVIDER","PROFESSIONAL","SPECASSESSMENT","ITEMPAYERSHARE","ITEM","TREATMENTDATE","CHRONIC"],
		"query": {
		"bool": {
		"must": [
		{"match": {"PROVIDERTYPE": "Pharmacy"}},
		{ "match": { "FOB":"Out-Patient" }},
		{ "match": { "STATUS":"Settled" }}
		]} }
		})
		response = requests.post('http://localhost:9200/nas_details/nas_claim_details/_search?scroll=1m', data=query)
		
		jsonDoc = json.loads(response.text)
		file = open('/opt/takaful/processed-data/pattern3-nas-input.csv','w')
		file.write("PROVIDER~PROFESSIONAL~SPECASSESSMENT~ITEMPAYERSHARE~ITEM~TREATMENTDATE~CHRONIC\n")
		for obj in jsonDoc['hits']['hits']:
			row="PH01~PH02~PH03~PH04~PH05~PH06~PH07"
			for key,val in  obj['fields'].iteritems():
				if  'ITEMPAYERSHARE'==key:
					row=row.replace('PH04',val[0])
				#if
				elif  'PROVIDER'==key:
					row=row.replace('PH01',val[0])
				#if
				elif  'PROFESSIONAL'==key:
					row=row.replace('PH02',val[0])
				#if
				elif  'SPECASSESSMENT'==key:
					row=row.replace('PH03',standardizeSpecAssesment(val[0]))
				#if
				elif  'ITEM'==key:
					row=row.replace('PH05',val[0].upper())
				#if
				elif  'TREATMENTDATE'==key:
					row=row.replace('PH06',val[0])
				#if
				elif  'CHRONIC'==key:
					row=row.replace('PH07',val[0])
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
		inputDF = pd.read_csv('/opt/takaful/processed-data/pattern3-nas-input.csv',delimiter='~')
		
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
		#inputDF['SPECASSESSMENT'] = inputDF.apply(lambda row : vlidateSpecAssesment(row['SPECASSESSMENT'].lower()),axis=1)		
		inputDF.dropna(subset=['PROVIDER','PROFESSIONAL','SPECASSESSMENT','ITEM'],inplace=True)
		
		inputDF = inputDF.drop(inputDF[inputDF['SPECASSESSMENT'].str.contains('NONE')].index)
		inputDF['ITEM'] = inputDF.apply(lambda row : str(row['ITEM']).strip(),axis=1)		
		
		#report 1 = 'PROVIDER','PROFESSIONAL','SPECASSESSMENT','ITEM'
		
		inputDF = inputDF.groupby(['PROVIDER','PROFESSIONAL','SPECASSESSMENT','ITEM']).agg({'ITEMPAYERSHARE':[np.sum,np.mean,np.max,np.min],'ITEM':np.size}).reset_index().rename(columns={'sum':'sum_finalamt','mean':'mean_finalamt','amin':'min_finalamt','amax':'max_finalamt','size':'occurance'})
		inputDF.columns =['PROVIDERNAME','ATTENDINGDOCTIRNAME','ICDDESCRIPTION','SERVICEDESCRIPTION','occurance','sum_finalamt','mean_finalamt','max_finalamt','min_finalamt']
		inputDF['PROVIDERGROUP']=''
		
		inputDF.to_csv('/opt/takaful/processed-data/pattern3-nas-report1.csv',header=True,index=False,index_label=False)
	#try
	except:
		traceback.print_exc()
	#except
	finally:
		print 'end of process...'
	#finally
#M2
###################################################################################################
#M3
def  loadAafia_Details_Data4mElasticSearch():
	try:
	
		query=json.dumps({
		"size": 9000000,
		"fields" :  ["TREATMENT DATE", "SERVICECODE","CLIENT GROUP","MEMBER NAME","CLAIM TYPE","FINAL AMT","PROVIDER GROUP","PROVIDER NAME","ATTENDING DOCTOR NAME","ICD DESCRIPTION","CURRENCY","SERVICEDESCRIPTION"],
		"query": {
		"bool": {
		"must": [	
		{ "match": { "IP\/OP":"OPD" }},
		{ "match": { "PAYMENTSTATUS":"PAID CLAIM" }}
		],
		"must_not": [
		{"match": {"CLAIM\/APPROVAL STATUS": "Rejected"}}
		]
		}
		}
		})
		response = requests.post('http://localhost:9200/aafiya_details/aafiya_claim_details/_search?scroll=9m', data=query)
		
		jsonDoc = json.loads(response.text)
		
		file = open('/opt/takaful/processed-data/pattern3-aafia-input.csv','w')
		file.write("TREATMENTDATE^SERVICECODE^CLIENTGROUP^MEMBERNAME^PROVIDERTYPE^CLAIMTYPE^FINALAMT^PROVIDERGROUP^PROVIDERNAME^ATTENDINGDOCTIRNAME^ICDDESCRIPTION^CURRENCY^SERVICEDESCRIPTION\n")
		for obj in jsonDoc['hits']['hits']:
			row="PH01^PH02^PH03^PH04^PH05^PH06^PH07^PH08^PH09^PH10^PH11^PH12^PH13"
			for key,val in  obj['fields'].iteritems():
				if  'TREATMENT DATE'==key:
					row=row.replace('PH01',val[0])
				#if
				elif  'SERVICECODE'==key:
					row=row.replace('PH02',val[0])
				#if
				elif  'CLIENT GROUP'==key:
					row=row.replace('PH03',val[0])
				#if
				elif  'MEMBER NAME'==key:
					row=row.replace('PH04',val[0])
				#if
				elif  'PROVIDER TYPE'==key:
					row=row.replace('PH05',val[0])
				#if
				elif  'CLAIM TYPE'==key:
					row=row.replace('PH06',val[0])
				#if
				elif  'FINAL AMT'==key:
					row=row.replace('PH07',val[0])
				#if
				elif  'PROVIDER GROUP'==key:
					row=row.replace('PH08',val[0])
				#if
				elif  'PROVIDER NAME'==key:
					row=row.replace('PH09',val[0])
				#if
				elif  'ATTENDING DOCTOR NAME'==key:
					row=row.replace('PH10',val[0])
				#if
				elif  'ICD DESCRIPTION'==key:
					row=row.replace('PH11',standardizeICD(val[0]))
				#if
				elif  'CURRENCY'==key:
					row=row.replace('PH12',val[0])
				#if
				elif  'SERVICEDESCRIPTION'==key:
					row=row.replace('PH13',val[0].upper())
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
#M3
###################################################################################################
#M4
def normalizeAafiaData():
	try:
		disease_lst = [line.strip().lower() for line in open('/opt/takaful/processed-data/concerned-disease-list.csv','r')]		
		inputDF = pd.read_csv('/opt/takaful/processed-data/pattern3-aafia-input.csv',delimiter='^')
		
		#inputDF = inputDF.replace(r'\s+',np.nan,regex=True)
		inputDF.dropna(subset=['PROVIDERGROUP','PROVIDERNAME','ATTENDINGDOCTIRNAME','ICDDESCRIPTION','SERVICEDESCRIPTION'],inplace=True)
		
		inputDF = inputDF.drop(inputDF[inputDF['ICDDESCRIPTION'].str.contains('NONE')].index)
		inputDF['SERVICEDESCRIPTION'] = inputDF.apply(lambda row : str(row['SERVICEDESCRIPTION']).strip(),axis=1)		
		
		#report 1 = CLIENTGROUP,PROVIDERTYPE,CLAIMTYPE,PROVIDERGROUP,PROVIDERNAME,ICDDESCRIPTION,SERVICEDESCRIPTION,ATTENDINGDOCTIRNAME
		
		inputDF = inputDF.groupby(['PROVIDERGROUP','PROVIDERNAME','ATTENDINGDOCTIRNAME','ICDDESCRIPTION','SERVICEDESCRIPTION']).agg({'FINALAMT':[np.sum,np.mean,np.max,np.min],'SERVICECODE':np.size}).reset_index().rename(columns={'sum':'sum_finalamt','mean':'mean_finalamt','amin':'min_finalamt','amax':'max_finalamt','size':'occurance'})
		
		inputDF.columns =['PROVIDERGROUP','PROVIDERNAME','ATTENDINGDOCTIRNAME','ICDDESCRIPTION','SERVICEDESCRIPTION','occurance','sum_finalamt','mean_finalamt','max_finalamt','min_finalamt']
	
		inputDF = inputDF[['PROVIDERNAME','ATTENDINGDOCTIRNAME','ICDDESCRIPTION','SERVICEDESCRIPTION','occurance','sum_finalamt','mean_finalamt','max_finalamt','min_finalamt','PROVIDERGROUP']] 
		def processPercentDisribution(row):
			try:
				total = inputDF.ix[(inputDF['PROVIDERGROUP']==row['PROVIDERGROUP']) & (inputDF['ATTENDINGDOCTIRNAME']==row['ATTENDINGDOCTIRNAME']) &  (inputDF['ICDDESCRIPTION']==row['ICDDESCRIPTION']) ]['occurance'].sum()
				return np.float(row['occurance'] * 100)/total
			#try
			except:
				traceback.print_exc()
			#except
		#def
		#inputDF['percentdistribution_provider_icd'] = inputDF.apply(lambda row: processPercentDisribution(row),axis=1)
		inputDF.to_csv('/opt/takaful/processed-data/pattern3-aafia-report1.csv',header=True,index=False,index_label=False)
	#try
	except:
		traceback.print_exc()
	#except
	finally:
		print 'end of process...'
	#finally
#M4
###################################################################################################
#M5
def loadNextCare4mElasticSearch():
	try:
		query=json.dumps({
		"size": 9000000,
		"fields" :  ["Provider","DischargeDate","ItemName", "SpecAssessment","PayerShare","Physician Name","ClaimCurrDesc","Service"],
		"query": {
		"bool": {
		"should":[{ "match": { "Service":"Pharmacy and Vaccinations" }},{ "match": { "Service":"Medicine" }}],
		"must": [
		{"match": {"ClaimStatus": "Settled"}},
		{ "match": { "FOB":"Out-Patient" }}
		],
		"must": [
		{"match": {"ProvChequeNumber": "Denied"}}
		]
		}
		}
		})
		response = requests.post('http://localhost:9200/nextcare3_p2/claim/_search?scroll=9m', data=query)
		
		jsonDoc = json.loads(response.text)
		
		file = open('/opt/takaful/processed-data/pattern3-nextcare-input.csv','w')
		file.write("PROVIDER^DISCHARGEDATE^ITEMNAME^SPECASSESSMENT^PAYERSHARE^DOCTORNAME^CURRENCY\n")
		for obj in jsonDoc['hits']['hits']:
			row="PH01^PH02^PH03^PH04^PH05^PH06^PH07"
			write2File =True
			for key,val in  obj['fields'].iteritems():
				if  'Service'==key:
					if val[0]!="Medicine" and val[0].upper()!='PHARMACY AND VACCINATIONS'  :
						write2File =False
						break
					#if
				#if
				if  'Provider'==key:
					row=row.replace('PH01',val[0])
				#if
				elif  'DischargeDate'==key:
					row=row.replace('PH02',val[0])
				#if
				elif  'ItemName'==key:
					itm=val[0].upper()
					itm = re.sub(r'PHARMACY AND VACCINATIONS-','',itm)
					itm = re.sub(r'MEDICINE-','',itm)
					row=row.replace('PH03',itm)
				#if
				elif  'SpecAssessment'==key:
					row=row.replace('PH04',standardizeSpec4NextCare(val[0]))
				#if
				elif  'PayerShare'==key:
					row=row.replace('PH05',val[0])
				#if
				elif  'Physician Name'==key:
					row=row.replace('PH06',val[0])
				#if
				elif  'ClaimCurrDesc'==key:
					row=row.replace('PH07',val[0])
				#if
			#for
			if write2File :
				file.write(str(row.encode('utf-8').strip())+'\n')
			#if	
		#for
		file.close()
	#try
	except:
		traceback.print_exc()
	#except
	finally:
		print 'end of process...'
	#finally
#M5
###################################################################################################
#M6
def normalizeNextCareData():
	try:
		inputDF = pd.read_csv('/opt/takaful/processed-data/pattern3-nextcare-input.csv',delimiter='^',error_bad_lines=False)
		
		inputDF.dropna(subset=['PROVIDER','DISCHARGEDATE','ITEMNAME','SPECASSESSMENT','PAYERSHARE','DOCTORNAME','CURRENCY'],inplace=True)
		inputDF['SPECASSESSMENT'] = inputDF.apply(lambda row : str(row['SPECASSESSMENT']).strip(),axis=1)		
		
		#report 1 = CLIENTGROUP,PROVIDERTYPE,CLAIMTYPE,PROVIDERGROUP,PROVIDERNAME,ICDDESCRIPTION,SERVICEDESCRIPTION,ATTENDINGDOCTIRNAME
		
		inputDF = inputDF.groupby(['PROVIDER','DOCTORNAME','SPECASSESSMENT','ITEMNAME']).agg({'PAYERSHARE':[np.sum,np.mean,np.max,np.min],'DISCHARGEDATE':np.size}).reset_index().rename(columns={'sum':'sum_finalamt','mean':'mean_finalamt','amin':'min_finalamt','amax':'max_finalamt','size':'occurance'})
		
		inputDF.columns =['PROVIDERNAME','ATTENDINGDOCTIRNAME','ICDDESCRIPTION','SERVICEDESCRIPTION','occurance','sum_finalamt','mean_finalamt','max_finalamt','min_finalamt']
		inputDF['PROVIDERGROUP']=''
		
		inputDF.to_csv('/opt/takaful/processed-data/pattern3-nextcare-report1.csv',header=True,index=False,index_label=False,sep='^')
	#try
	except:
		traceback.print_exc()
	#except
	finally:
		print 'end of process...'
	#finally
#M6
###################################################################################################
#M7
def mergeDatasets():
	try:
		nasDF = pd.read_csv('/opt/takaful/processed-data/pattern3-nas-report1.csv',error_bad_lines=False)
		nasDF['TPA']='NAS'
		aafiaDF = pd.read_csv('/opt/takaful/processed-data/pattern3-aafia-report1.csv',error_bad_lines=False)
		aafiaDF['TPA']='AAFIA'
		nextCareDF = pd.read_csv('/opt/takaful/processed-data/pattern3-nextcare-report1.csv',error_bad_lines=False,sep='^')
		nextCareDF['TPA']='NEXTCARE'
		
		final = [nasDF, aafiaDF, nextCareDF]
		result = pd.concat(final)
		result.to_csv('/opt/takaful/processed-data/consolicated-report1.csv',header=True,index=False,index_label=False)
	#try
	except:
		traceback.print_exc()
	#except
	finally:
		print 'end of process...'
	#finally
#M7
###################################################################################################
if __name__=='__main__':
	if sys.argv[1]=='1':
		loadNas_Details_Data4mElasticSearch()
	#if
	if sys.argv[1]=='2':
		normalizeNasData()
	#if
	if sys.argv[1]=='3':
		loadAafia_Details_Data4mElasticSearch()
	#if
	if sys.argv[1]=='4':
		normalizeAafiaData()
	#if
	if sys.argv[1]=='5':
		loadNextCare4mElasticSearch()
	#if
	if sys.argv[1]=='6':
		normalizeNextCareData()
	#if
	if sys.argv[1]=='7':
		mergeDatasets()
	#if
	if sys.argv[1]=='test':
		print standardizeICD('( J06.9-Acute upper respiratory infection; unspecified ) ( R05-Cough ) ( R50.9-Fever; unspecified )')
	#if
#if