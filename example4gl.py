import os
import pandas as pd
from datetime import datetime
import time
from google.cloud import storage
from io import BytesIO
from google.cloud import bigquery
from google.cloud import bigquery_storage
from google.oauth2 import service_account

start_time = time.time()
print("Starting POC - "+datetime.today().strftime('%Y-%m-%d ')+datetime.today().strftime('%H:%M:%S'))

#creating connection to bigquery
print("creating connection to bigquery - "+datetime.today().strftime('%Y-%m-%d ')+datetime.today().strftime('%H:%M:%S'))
credentials = service_account.Credentials.from_service_account_file(
'/opt/poc_fidw_4gl/secrets/hsbc-245009-fidwmx-dev-86f7a115d53e.json')

project_id = 'hsbc-245009-fidwmx-dev'
dataset = 'bdtesmul2gcp'
table = 'murex_t'
temp_table = 'tmurex_t'
table_id = """{}.{}.{}""".format(project_id,dataset,table)
temp_table_id = """{}.{}.{}""".format(project_id,dataset,temp_table)
output_file = 'LPMXMRXLM185D00021_MUREX.txt'
client = bigquery.Client(credentials= credentials,project=project_id)

#Start Getting temp table fields
bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)

query_string = """
SELECT
column_name
FROM
`hsbc-245009-fidwmx-dev`.{}.INFORMATION_SCHEMA.COLUMNS
WHERE
table_name="{}"
""".format(dataset,temp_table)

column_names = (
    client.query(query_string)
    .result()
    .to_dataframe(bqstorage_client=bqstorageclient)
)


x = column_names.to_string(header=False,index=False,index_names=True).split('\n')
field_names = [','.join(ele.split()) for ele in x]

if '_PARTITIONTIME' in field_names:
    print('removing unnecessary fields')
    field_names.remove('_PARTITIONTIME')

#End Getting temp table fields

#Reading file from google storage
print("Reading file from google storage - "+datetime.today().strftime('%Y-%m-%d ')+datetime.today().strftime('%H:%M:%S'))
try:
    storage_client = storage.Client(credentials= credentials,project=project_id)
    bucket_name = 'bucket-hsbc-fidw245009-ingest-data-main-dev'
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob('fidw_20201102_POC.prn')
    downloaded_blob = blob.download_as_string()
except:
    print('*** ¡No file exists! ***')

df = pd.read_csv(BytesIO(downloaded_blob), skiprows=2, skipfooter=2, sep="~", header=None, engine='python')

layout = len(df.columns)
vacio = len(df.index)

#Validating file layout
if layout != 138:
    raise Exception("*** ¡There is an error in the file layout! ***")
else:
    print("The layout of the file is correct")

#Validating file content
if vacio > 0:
    print("The file has content")
else:
    print("*** ¡The file is empty! ***")

#Use of the separa_murex_t.sh file. Replication process
df.to_csv('/opt/poc_fidw_4gl/inputs/fidw_20201102_POC.tmp',sep='~',index = False)
os.system('grep "^99" /opt/poc_fidw_4gl/inputs/fidw_20201102_POC.tmp|cut -f2 -d"~" > /opt/poc_fidw_4gl/inputs/fidw_20201102_POC.cif')
os.system('grep "^99" /opt/poc_fidw_4gl/inputs/fidw_20201102_POC.tmp|cut -f3 -d"~" > /opt/poc_fidw_4gl/inputs/fidw_20201102_POC.ksum')
os.system('sed "id;$d" /opt/poc_fidw_4gl/inputs/fidw_20201102_POC.tmp|cksum|cut -f1 -d" " > /opt/poc_fidw_4gl/inputs/fidw_20201102_POC.ksum_sis')
os.system('cat /opt/poc_fidw_4gl/inputs/fidw_20201102_POC.tmp |grep -v "TRADER" |cut -d"~" -f1-127,130-138 > /opt/poc_fidw_4gl/inputs/MUREX_T.nvo')

df = pd.read_csv('/opt/poc_fidw_4gl/inputs/MUREX_T.nvo',sep='~', skiprows=1,names=field_names)

print('******* Temporary Table (RDL) ********')
print(df)


#Replacing None values
df = df.replace([None], '', regex=True)

#Updating temporary table
print("Updating temp table - " + datetime.today().strftime('%Y-%m-%d ') + datetime.today().strftime('%H:%M:%S'))
get_table = client.get_table("""{}.{}.{}""".format(project_id,dataset,temp_table))
job_config = bigquery.LoadJobConfig( write_disposition =  "WRITE_TRUNCATE"
)

job = client.load_table_from_dataframe(
    df,get_table,job_config=job_config
)
job.result()

#Getting df from temp table
query_string = """
SELECT
*
FROM
`hsbc-245009-fidwmx-dev`.{}.{}
""".format(dataset,temp_table)

df = (
    client.query(query_string)
    .result()
    .to_dataframe(bqstorage_client=bqstorageclient)
)

#Start Tranformation process
# Missing columns are added ***
df['creation_date'] = df['creation_date'].astype(str)
print("Start Transformations - " + datetime.today().strftime('%Y-%m-%d ') + datetime.today().strftime('%H:%M:%S'))
acc = 1
for i,row in df.iterrows():
    tmpDate = datetime.today()
    df._set_value(i,'nrocns',str(acc))
    acc = acc+1
    df._set_value(i,'cvepai',None)
    df._set_value(i,'nroins',1)
    df._set_value(i,'nroemp',20)
    df._set_value(i,'cta_nominal_e',0)
    df._set_value(i,'aux_nominal_e',' ')
    df._set_value(i,'cod_nominal_e',0)
    df._set_value(i,'cta_grca_nominal_e',' ')
    df._set_value(i,'cta_nominal_r',0)
    df._set_value(i,'aux_nominal_r','  ')
    df._set_value(i,'cod_nominal_r',0)
    df._set_value(i,'cta_grca_nominal_r',' ')
    df._set_value(i,'cta_mtm_e',0)
    df._set_value(i,'aux_mtm_e',' ')
    df._set_value(i,'cod_mtm_e',0)
    df._set_value(i,'cta_grca_mtm_e',' ')
    df._set_value(i,'cta_mtm_r',0)
    df._set_value(i,'aux_mtm_r',' ')
    df._set_value(i,'cod_mtm_r',0)
    df._set_value(i,'cta_grca_mtm_r',' ')
    df._set_value(i,'pzomisdia',1)
    df._set_value(i,'tasvpzo',0)
    df._set_value(i,'fchtas',df._get_value(i, 'sysdate1'))
    df._set_value(i,'counterpart_treats','')
    df._set_value(i,'fchreg',tmpDate.strftime('%Y-%m-%d'))
    df._set_value(i,'exercise',' ')
    df._set_value(i,'cvepai',None)
    df._set_value(i,'nroins',1)
    df._set_value(i,'nroemp',20)
    df._set_value(i,'cta_nominal_e',0)
    df._set_value(i,'aux_nominal_e',' ')
    df._set_value(i,'cod_nominal_e',0)
    df._set_value(i,'cta_grca_nominal_e',' ')
    df._set_value(i,'cta_nominal_r',0)
    df._set_value(i,'aux_nominal_r',' ')
    df._set_value(i,'cod_nominal_r',0)
    df._set_value(i,'cta_grca_nominal_r',' ')
    df._set_value(i,'cta_mtm_e',0)
    df._set_value(i,'aux_mtm_e',' ')
    df._set_value(i,'cod_mtm_e',0)
    df._set_value(i,'cta_grca_mtm_e',' ')
    df._set_value(i,'cta_mtm_r',0)
    df._set_value(i,'aux_mtm_r',' ')
    df._set_value(i,'cod_mtm_r',0)
    df._set_value(i,'cta_grca_mtm_r',' ')
    df._set_value(i,'pzomisdia',1)
    df._set_value(i,'tasvpzo',0)
    df._set_value(i,'tasvpzo',0)
    if df._get_value(i, 'plmarvalrevdis')=='-':
        df._set_value(i,'plmarvalrevdis',0)
    if df._get_value(i, 'plmarvalrevnon')=='-':
        df._set_value(i,'plmarvalrevnon',0)
    if df._get_value(i, 'plfutprocapdis')=='-':
        df._set_value(i,'plfutprocapdis',0)
    if df._get_value(i, 'plfutprorevnon')=='-':
        df._set_value(i,'plfutprorevnon',0)
    if df._get_value(i, 'plpascasrevfin')=='-':
        df._set_value(i,'plpascasrevfin',0)
    if df._get_value(i, 'plpascasrevno')=='-':
        df._set_value(i,'plpascasrevno',0)
    if df._get_value(i, 'markspo2')=='-':
        df._set_value(i,'markspo2',0)
    if (df._get_value(i, 'creation_date'))[3:4]=='/':
        df._set_value(i,'creation_date','20{}{}{}'.format((df._get_value(i, 'creation_date'))[7:8],(df._get_value(i, 'creation_date'))[4:5],(df._get_value(i, 'creation_date'))[1:2]))
    if df._get_value(i, 'modification_date')=='*.*':
        df._set_value(i,'modification_date','')
    df._set_value(i,'horreg',tmpDate.strftime('%HH:%MM:%SS'))
    df._set_value(i,'usureg','')
    df._set_value(i,'opctype','')
    df._set_value(i,'settype','')
    df._set_value(i,'caltype','')

df._set_value(1,'fchinf','add')
df['fchinf'] = df['fchinf'].astype(str)
df['sysdate1'] = df['sysdate1'].astype(str)
df['fchinf'] = datetime.strptime(df._get_value(1, 'sysdate1'),'%Y%m%d').strftime('%Y/%m/%d')

print("End Transformations - " + datetime.today().strftime('%Y-%m-%d ') + datetime.today().strftime('%H:%M:%S'))

#Start Getting table fields

query_string = """
SELECT
column_name
FROM
`hsbc-245009-fidwmx-dev`.{}.INFORMATION_SCHEMA.COLUMNS
WHERE
table_name="{}"
""".format(dataset,table)

column_names = (
    client.query(query_string)
    .result()
    .to_dataframe(bqstorage_client=bqstorageclient)
)


x = column_names.to_string(header=False,index=False,index_names=True).split('\n')
field_names = [','.join(ele.split()) for ele in x]

if '_PARTITIONTIME' in field_names:
    print('removing unnecessary fields')
    field_names.remove('_PARTITIONTIME')

df['nrocns'] = df['nrocns'].astype(int)
df = df[field_names]
print('************* Final Table **********')
df = df.replace('', None, regex=True)
print(df)

fchinf = df._get_value(1, 'fchinf')
#Updating table
print("Updating table - " + datetime.today().strftime('%Y-%m-%d ') + datetime.today().strftime('%H:%M:%S'))
get_table = client.get_table("""{}.{}.{}""".format(project_id,dataset,table))
job_config = bigquery.LoadJobConfig( write_disposition =  "WRITE_TRUNCATE"
)#Is required remove the "WRITE_TRUNCATE" config for PROD

job = client.load_table_from_dataframe(
    df,get_table,job_config=job_config
)
job.result()

#Start Replication of the process which generates the file to send to GREP (COEF5315.4gl)
print("Start Replication of the process which generates the file to send to GREP (COEF5315.4gl) - " + datetime.today().strftime('%Y-%m-%d ') + datetime.today().strftime('%H:%M:%S'))
f_table = 'murex_temp'
final_table = """{}.{}.{}""".format(project_id,dataset,f_table)
job_config = bigquery.QueryJobConfig(destination=final_table,write_disposition =  "WRITE_TRUNCATE")

sql  = f"""select 
fchinf,nrocns,fulltrnnb,trninitial,trninternal,entity,portfolio,trader,bosignature,
FORMAT_DATE('%Y/%m/%d',PARSE_DATE('%Y%m%d', CAST(sysdate1 AS STRING))) as sysdate1,
systime,trndate,trnfmly,trngroup,trntype,fxcontrmark,buy_sell,call_put,ameri_euro,delive_settle,
descomptype,nomi_curren1,instrument,nomi_curren2,counterpart,thirdparty,countcountry,country,
FORMAT_DATE('%Y/%m/%d',PARSE_DATE('%Y%m%d', CAST(datefirtsflow AS STRING))) as datefirtsflow,
FORMAT_DATE('%Y/%m/%d',PARSE_DATE('%Y%m%d', CAST(datelastflow AS STRING))) as datelastflow,
FORMAT_DATE('%Y/%m/%d',PARSE_DATE('%Y%m%d', CAST(datecalc_end AS STRING))) as datecalc_end,
FORMAT_DATE('%Y/%m/%d',PARSE_DATE('%Y%m%d', CAST(datecalc_star AS STRING))) as datecalc_star,
FORMAT_DATE('%Y/%m/%d',PARSE_DATE('%Y%m%d', CAST(dateexpiry AS STRING))) as dateexpiry,
FORMAT_DATE('%Y/%m/%d',PARSE_DATE('%Y%m%d', CAST(datevalue AS STRING))) as datevalue,
CASE
WHEN CAST(dateunderdeli AS STRING) = '0' THEN '' ELSE FORMAT_DATE('%Y/%m/%d',PARSE_DATE('%Y%m%d', CAST(dateunderdeli AS STRING))) END dateunderdeli,
CASE
WHEN CAST(dateunderexp AS STRING) = '0' THEN '' ELSE FORMAT_DATE('%Y/%m/%d',PARSE_DATE('%Y%m%d', CAST(dateunderexp AS STRING))) END dateunderexp,
CASE
WHEN CAST(dateunderopt AS STRING) = '0' THEN '' ELSE FORMAT_DATE('%Y/%m/%d',PARSE_DATE('%Y%m%d', CAST(dateunderopt AS STRING))) END dateunderopt,
FORMAT_DATE('%Y/%m/%d',PARSE_DATE('%Y%m%d', CAST(initialpayment AS STRING))) as initialpayment,
convinitpaym,convinicurren,discounmark,discounclose,discountheo,nomicurren,initiqty_sig,nomiamount,livequan_sig,
equivquan,equivquanuni,fxhisspot,fxhisspotacc,salesmargin,salescurren,spotnotati,spot_fwd,strike,
strikenotati,initialpaycurr,initialprice,initialpay,delta,gamma,rho,rhof,theta,vanna,
vega,volga,volm,deltacurren,fxbasecurren,fxcallputcurr,fxcontraccal,fxcontracdelt,fxcontracgam,grosecopl0,
grosecopl2,financashbal,plcurren,netecompl2,plmarvalcapdis,plmarvalcapnon,plmarvalrevdis,plmarvalrevnon,plfutprocapdis,plfutprorevnon,
plpascascapfi,plpascascapno,plpascasrevfin,plpascasrevno,nondismarkval,nondismarkclo,nondismarkthe,nonfinacasbal,nonfinrealcasb,presenvaleffec,realiedcapga12,
uniclospricethe,unreacapgain,unreareve,capitalgain2,markra1,markra2,markspo,markspo2,markvol,mp_spotc2,mp_swap2,broker,
brokerage,broker1,brokerage1,broker2,brokerage2,broker3,brokerage3,cvepai,nroins,nroemp,cta_nominal_e,aux_nominal_e,cod_nominal_e,cta_grca_nominal_e,cta_nominal_r,aux_nominal_r,cod_nominal_r,
cta_grca_nominal_r,cta_mtm_e,aux_mtm_e,cod_mtm_e,cta_grca_mtm_e,cta_mtm_r,aux_mtm_r,cod_mtm_r,cta_grca_mtm_r,pzomisdia,tasvpzo,fchtas,exercise,opctype,settype,caltype,flex_type,
barri_type,barri1,barri2,calen,deliv_set,trade_stat,strategy,creation_date,modification_date,put_ccy,put_amount,call_ccy,call_amount,nb_lti,settlement_ccy,datefirstfix,payment,flow_type,
barrier_style from {table_id} where fchinf = '{fchinf}'"""

query_job = client.query(sql, job_config=job_config)
query_job.result()  # Wait for the job to complete.
print("Query results loaded to the table {}".format(final_table))

#Creating the file to send to GREP
print("Creating the file to Send to GREP - "+datetime.today().strftime('%Y-%m-%d ')+datetime.today().strftime('%H:%M:%S'))
bucket_name = 'bucket-hsbc-fidw245009-ingest-rr-data-main-dev'

destination_uri = "gs://{}/{}".format(bucket_name, output_file)
dataset_ref = bigquery.DatasetReference(project_id, dataset)
table_ref = dataset_ref.table(f_table)

job_config = bigquery.job.ExtractJobConfig(print_header=False,field_delimiter='|')
extract_job = client.extract_table(
    table_ref,
    destination_uri,
    # Location must match that of the source table.
    #location="US",
    job_config=job_config
)  # API request
extract_job.result()  # Waits for job to complete.

print(
    "Exported {}:{}.{} to {}".format(project_id, dataset, table, destination_uri)
)
print("Ending POC - "+datetime.today().strftime('%Y-%m-%d ')+datetime.today().strftime('%H:%M:%S'))
#Printing the process execution time in minutes
print("--- {} minutes ---".format((time.time() - start_time)/60))
