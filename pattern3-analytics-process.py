import traceback
import json
from elasticsearch import Elasticsearch
import sys
import requests
import pandas as pd
import spacy
import re
import numpy as np
from datetime import datetime
import StringIO

#thsi process is pattern 3 for takaful search - this pattern identifies medicines vs issues.
 
#grep pattern3-nextcare-report1.csv -e 'PHARMACY AND VACCINATIONS'
#COMMON
###################################################################################################
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

currentTime = datetime.utcnow()
###################################################################################################
#M1
def  loadNas_Details_Data4mElasticSearch():
	try:
		query=json.dumps({
		"size": 9000000,
		"fields" :  ["PROVIDER","PROFESSIONAL","SPECASSESSMENT","ITEMPAYERSHARE","ITEM","TREATMENTDATE","CHRONIC","BENEFICIARY","EMPID"],
		"query": {
		"bool": {
		"must": [
		{"match": {"PROVIDERTYPE": "Pharmacy"}},
		{ "match": { "FOB":"Out-Patient" }},
		{ "match": { "STATUS":"Settled" }}
		]} }
		})
		response = requests.post('http://localhost:9200/nas_details/nas_claim_details/_search?scroll=9m', data=query)
		
		jsonDoc = json.loads(response.text)
		file = open('/opt/takaful/processed-data/pattern3-nas-input.csv','w')
		file.write("PROVIDER~PROFESSIONAL~SPECASSESSMENT~ITEMPAYERSHARE~ITEM~TREATMENTDATE~CHRONIC~BENEFICIARY~BENID\n")
		for obj in jsonDoc['hits']['hits']:
			row="PH01~PH02~PH03~PH04~PH05~PH06~PH07~PH08~PH09"
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
				elif  'BENEFICIARY'==key:
					row=row.replace('PH08',val[0])
				#if
				elif  'EMPID'==key:
					row=row.replace('PH09',val[0])
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
		"fields" :  ["TREATMENT DATE", "SERVICECODE","CLIENT GROUP","MEMBER NAME","CLAIM TYPE","FINAL AMT","PROVIDER GROUP","PROVIDER NAME","ATTENDING DOCTOR NAME","ICD DESCRIPTION","CURRENCY","SERVICEDESCRIPTION","PROVIDER TYPE","MEMBER ID NO"],
		"query": {
		"bool": {
		"must": [
		{ "match": { "IP/OP":"OPD" }},
		{ "match": { "PAYMENTSTATUS":"PAID CLAIM"}},
		{ "match": { "PROVIDER TYPE":"PHARMACY"}},
		],
		"must_not": [
		{"match": {"CLAIM\/APPROVAL STATUS": "Rejected"}}
		]} }
		})
		response = requests.post('http://localhost:9200/aafiya_details/aafiya_claim_details/_search?scroll=9m', data=query)
		
		jsonDoc = json.loads(response.text)
		
		file = open('/opt/takaful/processed-data/pattern3-aafia-input.csv','w')
		file.write("TREATMENTDATE^SERVICECODE^CLIENTGROUP^MEMBERNAME^PROVIDERTYPE^CLAIMTYPE^FINALAMT^PROVIDERGROUP^PROVIDERNAME^ATTENDINGDOCTIRNAME^ICDDESCRIPTION^CURRENCY^SERVICEDESCRIPTION^BENID\n")
		for obj in jsonDoc['hits']['hits']:
			row="PH01^PH02^PH03^PH04^PH05^PH06^PH07^PH08^PH09^PH10^PH11^PH12^PH13^PH14"
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
				elif  'MEMBER ID NO'==key:
					row=row.replace('PH14',val[0])
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
		"fields" :  ["Provider","DischargeDate","ItemName", "SpecAssessment","PayerShare","Physician Name","ClaimCurrDesc","Service","BenefName","CardNumber"],
		"query": {
		"bool": {
		"should":[{ "match": { "Service":"Pharmacy and Vaccinations" }},{ "match": { "Service":"Medicine" }}],
		"must": [
		{"match": {"ClaimStatus": "Settled"}},
		{ "match": { "FOB":"Out-Patient" }}
		],
		"must_not": [
		{"match": {"ProvChequeNumber": "Denied"}}
		]
		}
		}
		})
		response = requests.post('http://localhost:9200/nextcare3_p2/claim/_search?scroll=9m', data=query)
		
		jsonDoc = json.loads(response.text)
		
		file = open('/opt/takaful/processed-data/pattern3-nextcare-input.csv','w')
		file.write("PROVIDER^DISCHARGEDATE^ITEMNAME^SPECASSESSMENT^PAYERSHARE^DOCTORNAME^CURRENCY^BENEFICIARY^BENID\n")
		for obj in jsonDoc['hits']['hits']:
			row="PH01^PH02^PH03^PH04^PH05^PH06^PH07^PH08^PH09"
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
				elif  'BenefName'==key:
					row=row.replace('PH08',val[0])
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
				elif  'CardNumber'==key:
					row=row.replace('PH09',val[0])
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
#M8
def analyzeDSvsDrugAnomoly():
	try:	
		inputDF = pd.read_csv('/opt/takaful/processed-data/consolicated-report1.csv')
		
		def normalizeICD(val):
			try:
				if val.isdigit():
					if val == 0:
						return '0'
					#if
					else :
						return val
					#else
				#if
				else :
					return val
				#if
			#try
			except:
				traceback.print_exc()
			#except
		#def
		inputDF['ICDDESCRIPTION']=inputDF.apply(lambda row: normalizeICD(row['ICDDESCRIPTION']),axis=1)
		
		processdDF1 = inputDF.groupby(['ICDDESCRIPTION'])['ATTENDINGDOCTIRNAME'].nunique().reset_index()
		processdDF1.columns=['ICDDESCRIPTION','UNIQUEDOCCOUNT']
		processdDF1.set_index(['ICDDESCRIPTION'])
		
		processdDF2 = inputDF.groupby(['ICDDESCRIPTION','SERVICEDESCRIPTION'])['ATTENDINGDOCTIRNAME'].nunique().reset_index()
		processdDF2.columns=['ICDDESCRIPTION','SERVICEDESCRIPTION','UNIQUEDSDOCCOUNT']
		processdDF2.set_index(['ICDDESCRIPTION'])
		def detectAnomoly(row):
			try:
				val = np.float(row['UNIQUEDSDOCCOUNT'])/int(row['UNIQUEDOCCOUNT'])*100
				if val <= 10:
					return 'T'
				#if
				else:
					return 'F'
				#if
			#try
			except:
				traceback.print_exc()
			#except	
		#def
		processedDF3 = pd.merge(processdDF2,processdDF1,on ='ICDDESCRIPTION',how='inner')
		processedDF3['ISANOMOLY'] = processedDF3.apply(lambda row: detectAnomoly(row), axis=1)
		
		processedDF3.to_csv('/opt/takaful/processed-data/pattern3-DSvsDrugAnomoly.csv',header=True,index=False,index_label=False)
	#try
	except:
		traceback.print_exc()
	#except
	finally:
		print 'end of process...'
	#finally
#M8
###################################################################################################
#M9
def analyzeDrugAbuse():
	try:
		restrictiveDrug = [line.strip() for line in open('/opt/takaful/processed-data/restrictive-drugs.txt','r')]
		
		nasDF = pd.read_csv('/opt/takaful/processed-data/pattern3-nas-input.csv',delimiter='~')
		nasDF = nasDF[['SPECASSESSMENT','ITEM','BENEFICIARY','BENID','ITEMPAYERSHARE','TREATMENTDATE']]
		nasDF['TPA']='NAS'
		nasDF.dropna(subset=['SPECASSESSMENT','ITEM','BENEFICIARY','BENID','ITEMPAYERSHARE','TREATMENTDATE'],inplace=True)
		
		def evaluateRestrictiveDrug(val):
			try:
				tmp = [restrictiveDrug[i].upper() for i in range(len(restrictiveDrug)) if  restrictiveDrug[i].upper() in val.upper() ]
				if len(tmp)>0:
					return tmp[0]
				#if
				else:
					return 'NONE'
				#else
			#try
			except:
				traceback.print_exc()
			#except
		#def
		nasDF['DRUG'] = nasDF.apply(lambda row : evaluateRestrictiveDrug(row['ITEM']),axis=1)		
		nasDF = nasDF.drop(nasDF[nasDF['DRUG'].str.contains('NONE')].index)
		
		def parseTimeNAS(val):
			try:
				obj = datetime.strptime(val,'%d-%b-%y')
				diff =  (currentTime -obj).days
				if diff <=360:
					return  str(obj.year)+str(obj.month).zfill(2) 
				#if
				else:
					return 'NONE'
				#else
			#try
			except:
				traceback.print_exc()
			#except
		#def
		nasDF['DRUGDATE'] = nasDF.apply(lambda row : parseTimeNAS(row['TREATMENTDATE']),axis=1)		
		nasDF = nasDF.drop(nasDF[nasDF['DRUGDATE'].str.contains('NONE')].index)
		
		aafiaDF = pd.read_csv('/opt/takaful/processed-data/pattern3-aafia-input.csv',delimiter='^')
		aafiaDF = aafiaDF[['ICDDESCRIPTION','SERVICEDESCRIPTION','MEMBERNAME','BENID','FINALAMT','TREATMENTDATE']]
		aafiaDF['TPA']='AAFIA'
		aafiaDF.dropna(subset=['ICDDESCRIPTION','SERVICEDESCRIPTION','MEMBERNAME','BENID','FINALAMT','TREATMENTDATE'],inplace=True)
		aafiaDF['DRUG'] = aafiaDF.apply(lambda row : evaluateRestrictiveDrug(row['SERVICEDESCRIPTION']),axis=1)		
		aafiaDF = aafiaDF.drop(aafiaDF[aafiaDF['DRUG'].str.contains('NONE')].index)
		def parseTimeAafia(val):
			try:
				obj = datetime.strptime(val,'%m-%d-%y')
				diff =  (currentTime -obj).days
				if diff <=360:
					return  str(obj.year)+str(obj.month).zfill(2) 
				#if
				else:
					return 'NONE'
				#else
			#try
			except:
				traceback.print_exc()
			#except
		#def
		aafiaDF['DRUGDATE'] = aafiaDF.apply(lambda row : parseTimeAafia(row['TREATMENTDATE']),axis=1)		
		aafiaDF = aafiaDF.drop(aafiaDF[aafiaDF['DRUGDATE'].str.contains('NONE')].index)
		aafiaDF.columns = ['SPECASSESSMENT','ITEM','BENEFICIARY','BENID','ITEMPAYERSHARE','TREATMENTDATE','TPA','DRUGDATE','DRUG']
		
		nextDF = pd.read_csv('/opt/takaful/processed-data/pattern3-nextcare-input.csv',delimiter='^',error_bad_lines=False)
		nextDF = nextDF[['SPECASSESSMENT','ITEMNAME','BENEFICIARY','BENID','PAYERSHARE','DISCHARGEDATE']]
		nextDF['TPA']='NEXTCARE'
		nextDF.dropna(subset=['SPECASSESSMENT','ITEMNAME','BENEFICIARY','BENID','PAYERSHARE','DISCHARGEDATE'],inplace=True)
		nextDF['DRUG'] = nextDF.apply(lambda row : evaluateRestrictiveDrug(row['ITEMNAME']),axis=1)		
		nextDF = nextDF.drop(nextDF[nextDF['DRUG'].str.contains('NONE')].index)
		def parseTimeNextCare(val):
			try:
				obj = datetime.strptime(val,'%d/%m/%Y')
				diff =  (currentTime -obj).days
				if diff <=360:
					return  str(obj.year)+str(obj.month).zfill(2) 
				#if
				else:
					return 'NONE'
				#else
			#try
			except:
				traceback.print_exc()
			#except
		#def
		nextDF['DRUGDATE'] = nextDF.apply(lambda row : parseTimeNextCare(row['DISCHARGEDATE']),axis=1)		
		nextDF = nextDF.drop(nextDF[nextDF['DRUGDATE'].str.contains('NONE')].index)
		nextDF.columns = ['SPECASSESSMENT','ITEM','BENEFICIARY','BENID','ITEMPAYERSHARE','TREATMENTDATE','TPA','DRUGDATE','DRUG']
		
		final = [nasDF, aafiaDF, nextDF]
		result = pd.concat(final)
				
		result1 = result.groupby(['BENID','DRUGDATE']).agg({'ITEMPAYERSHARE':np.sum,'TPA':np.size}).reset_index().rename(columns={'sum':'TOTALCOST','size':'RESTRICTEDDRUGCOUNT'})
		result1.columns =['BENID','DRUGDATE','TOTALCOST','RESTRICTEDDRUGCOUNT']
		
		def removeUnrestrictedDrug(val):
			try:
				if int(val)>=1:
					return val
				#if
				else:
					return 0
				#else
			#try
			except:
				traceback.print_exc()
			#except:
		#def
		result1['RESTRICTEDDRUGCOUNT']= result1.apply(lambda row: removeUnrestrictedDrug(row['RESTRICTEDDRUGCOUNT']),axis=1)
		result1 = result1.drop(result1[(result1['RESTRICTEDDRUGCOUNT']==0)].index)
		result1.to_csv('/opt/takaful/processed-data/pattern3-drug-abuse-calculated.csv',header=True,index=False,index_label=False)
		
		for key in result['BENID'].unique():
			lst = result.ix[(result['BENID']==key)]['DRUGDATE'].tolist()
			if len(lst)>=3:
				print key
			#if	
		#for
		
		finalLst = ['0C5ACCF7EAA0850E','0F619AF9E6AA0B04','103407','537774A603A05E65','54AA05E4A9F5477A','5BC2CA28EBAB007E','7E62629EB7985BB0','844F99C086654EB0','D4523FC11071BEE3','E9B7FFED430E6817']
		def validateRDAbuseFlag(val):
			try:
				if any(str(val) == item for item in finalLst):
					return 'T'
				#if
				return 'F'
			#try
			except:
				traceback.print_exc()
			#except
		#def
		result['isRestrictedDrugAbuse'] = result.apply(lambda row: validateRDAbuseFlag(row['BENID']),axis=1)
		result = result.drop(result[result['isRestrictedDrugAbuse'].str.contains('F')].index)
		result.to_csv('/opt/takaful/processed-data/pattern3-drug-abuse-total.csv',header=True,index=False,index_label=False)
	#try
	except:
		traceback.print_exc()
	#except
	finally:
		print 'end of process...'
	#finally
#M9
###################################################################################################
#M10
def createDrugAbuseReport():
	try:
		inputDF  = pd.read_csv('/opt/takaful/processed-data/pattern3-drug-abuse-total.csv')
		buff = StringIO.StringIO()
		buff.write("TPA~PolicyNumber~ClaimNumber~ClientGroup~Client~MemberId~MemberName~ProviderType~ProviderGroup~Provider~Diagnosis~ IP_OP~ServiceCode~ServiceDetail~ServiceType~Doctor~TreatmentDate~ClaimStatus~PaymentStatus~PaidAmount~Currency~IsRestrictedDrugAbuse\n")
		for index,row in inputDF.iterrows():
			BENEFICIARY,BENID,DRUG,DRUGDATE,ITEM,ITEMPAYERSHARE,SPECASSESSMENT,TPA,TREATMENTDATE,isRestrictedDrugAbuse = row
			if TPA=='NAS':
				query=json.dumps({
				"size": 9000000,
				"fields" :  ["POLICYNUMBER","CLAIMID","MASTERCONTRACT","CONTRACT","EMPID","BENEFICIARY","PROVIDERTYPE","PROVIDER","SPECASSESSMENT","ITEM","PROFESSIONAL","TREATMENTDATE","STATUS","ITEMPAYERSHARE","CURRENCY"],
				"query": {
				"bool": {
				"must": [
				{"match": {"PROVIDERTYPE": "Pharmacy"}},
				{ "match": { "FOB":"Out-Patient" }},
				{ "match": { "STATUS":"Settled" }},
				{ "match": { "EMPID":BENID }},
				{ "match": { "ITEM":ITEM }},
				{ "match": { "TREATMENTDATE":TREATMENTDATE }},
				]} }
				})
				response = requests.post('http://localhost:9200/nas_details/nas_claim_details/_search?scroll=9m', data=query)
				
				jsonDoc = json.loads(response.text)
				for obj in jsonDoc['hits']['hits']:
					line = "NAS~PH01~PH02~PH03~PH04~PH05~PH06~PH07~NA~PH08~PH09~Out-Patient~NA~PH10~MEDICINE~PH11~PH12~NA~PH13~PH14~PH15~Y"
					for key,val in  obj['fields'].iteritems():
						if  'POLICYNUMBER'==key:
							line=line.replace('PH01',val[0])
						#if
						elif  'CLAIMID'==key:
							line=line.replace('PH02',val[0])
						#if
						elif  'MASTERCONTRACT'==key:
							line=line.replace('PH03',val[0])
						#if
						elif  'CONTRACT'==key:
							line=line.replace('PH04',val[0])
						#if
						elif  'BENEFICIARY'==key:
							line=line.replace('PH05',val[0])
							line=line.replace('PH06',val[0])
						#if
						elif  'PROVIDERTYPE'==key:
							line=line.replace('PH07',val[0])
						#if
						elif  'PROVIDER'==key:
							line=line.replace('PH08',val[0])
						#if
						elif  'SPECASSESSMENT'==key:
							line=line.replace('PH09',val[0])
						#if
						elif  'ITEM'==key:
							line=line.replace('PH10',val[0])
						#if
						elif  'PROFESSIONAL'==key:
							line=line.replace('PH11',val[0])
						#if
						elif  'TREATMENTDATE'==key:
							line=line.replace('PH12',val[0])
						#if
						elif  'STATUS'==key:
							line=line.replace('PH13',val[0])
						#if
						elif  'ITEMPAYERSHARE'==key:
							line=line.replace('PH14',val[0])
						#if
						elif  'CURRENCY'==key:
							line=line.replace('PH15',val[0])
						#if
					#for
					buff.write( line +'\n')
				#for		
			#if
			if TPA=='NEXTCARE':
				query=json.dumps({
				"size": 9000000,
				"fields" :  ["PolicyNbr","InvoiceNbr","MasterContract","Contract","CardNumber","BenefName","Provider","SpecAssessment","Service Item","ItemName","Physician Name","DischargeDate","ClaimStatus","PayerShare","ClaimCurrDesc"],
				"query": {
				"bool": {
				"should":[{ "match": { "Service":"Pharmacy and Vaccinations" }},{ "match": { "Service":"Medicine" }}],
				"must": [
				{"match": {"ClaimStatus": "Settled"}},
				{ "match": { "FOB":"Out-Patient" }},
				{ "match": { "CardNumber":BENID }},
				{ "match": { "ItemName":ITEM }},
				{ "match": { "DischargeDate":TREATMENTDATE }},
				],
				"must_not": [
				{"match": {"ProvChequeNumber": "Denied"}}
				]
				}
				}
				})
				response = requests.post('http://localhost:9200/nextcare3_p2/claim/_search?scroll=9m', data=query)
				jsonDoc = json.loads(response.text)
				for obj in jsonDoc['hits']['hits']:
					line  ="NEXTCARE~PH01~PH02~PH03~PH04~PH05~PH06~NA~NA~PH07~PH08~Out-Patient~PH09~PH10~MEDICINE~PH11~PH12~PH13~PH14~PH15~Y"
					for key,val in  obj['fields'].iteritems():
						if  'PolicyNbr'==key:
							line=line.replace('PH01',val[0])
						#if
						elif  'InvoiceNbr'==key:
							line=line.replace('PH02',val[0])
						#if
						elif  'MasterContract'==key:
							line=line.replace('PH03',val[0])
						#if
						elif  'Contract'==key:
							line=line.replace('PH04',val[0])
						#if
						elif  'CardNumber'==key:
							line=line.replace('PH05',val[0])
						#if
						elif  'BenefName'==key:
							line=line.replace('PH06',val[0])
						#if
						elif  'Provider'==key:
							line=line.replace('PH07',val[0])
						#if
						elif  'SpecAssessment'==key:
							line=line.replace('PH08',val[0])
						#if
						elif  'Service Item'==key:
							line=line.replace('PH09',val[0])
						#if
						elif  'ItemName'==key:
							line=line.replace('PH10',val[0])
						#if
						elif  'Physician Name'==key:
							line=line.replace('PH11',val[0])
						#if
						elif  'DischargeDate'==key:
							line=line.replace('PH12',val[0])
						#if
						elif  'ClaimStatus'==key:
							line=line.replace('PH13',val[0])
						#if
						elif  'PayerShare'==key:
							line=line.replace('PH14',val[0])
						#if
						elif  'ClaimCurrDesc'==key:
							line=line.replace('PH15',val[0])
						#if
					#for
					buff.write( line +'\n')
				#for
			#if
		#for
		file = open('/opt/takaful/processed-data/pattern3-drug-abuse-report.csv','w')
		file.write(buff.getvalue())
		file.close()
		dataDF = pd.read_csv('/opt/takaful/processed-data/pattern3-drug-abuse-report.csv',sep='~')
		dataDF.to_csv('/opt/takaful/processed-data/pattern3-drug-abuse-report.csv',header=True,index=False,index_label=False)
	#try
	except:
		traceback.print_exc()
	#except
#M10
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
	if sys.argv[1]=='8':
		analyzeDSvsDrugAnomoly()
	#if
	if sys.argv[1]=='9':
		analyzeDrugAbuse()
	#if
	if sys.argv[1]=='10':
		createDrugAbuseReport()
	#if
	if sys.argv[1]=='test':
		print standardizeICD('( J06.9-Acute upper respiratory infection; unspecified ) ( R05-Cough ) ( R50.9-Fever; unspecified )')
	#if
#if