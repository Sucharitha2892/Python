import numpy as np
import pandas as pd
import sys
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import AlreadyExists, NotFound

class Coef5307:

    def __init__(self,skiprows,vfetcha):
        self.common = Common()
        self.table = 'fwp'
        self.temp_table = 'tfwp'
        self.table_id = """{}.{}.{}""".format(self.common.project_id, self.common.dataset, self.table)
        self.temp_table_id = """{}.{}.{}""".format(self.common.project_id, self.common.dataset, self.temp_table)
        self.input_file = 'MXTTFWP.txt'
        # self.vfetcha = sys.argv[1]
        self.vfetcha = vfetcha
        self.fgmont ='FgMont'
        self.fgmont_id = """{}.{}.{}""".format(self.common.project_id,self.common.dataset, self.fgmont)
        credentials = service_account.Credentials.from_service_account_file(
            '/opt/poc_fidw_4gl/secrets/hsbc-245009-fidwmx-dev-86f7a115d53e.json')
        self.client = bigquery.Client(credentials=credentials, project="project_id")

    def ObtPais(self,vnPais):
        self.vnPais =vnPais
        if vnPais==1:
            self.vnPaisT=11
        elif vnPais==2:
            self.vnPaisT=12
        return self.vnPaisT

    def ObtMonedaT(self,vnMoneda, vnPais):
        self.vnMoneda = vnMoneda
        self.vnPais = vnPais
        query = 'SELECT FgmMonTre FROM FgMont WHERE FgmMonSic = {} AND FgmPaiTre = {}'.format(self.vnMoneda, self.vnPais)
        try:
            df2 = self.client.query(query).result().to_dataframe()
            self.vnMonT = df2["FgmMonTre"].values.tolist()
        except NotFound:
            print(" ")
        return self.vnMonT

    def run(self):
        try:
            df = self.common.getDataFrameBq(self.temp_table)
            query = 'select'
            for i, row in df.iterrows():
                df._set_value(i, 'fchinf', self.vfecha)  # may be it'll change later
                df._set_value(i, 'nrocns', VConsecut)
                df._set_value(i, 'mtorecibi', df._get_value(i, 'mtorecibi')/100)
                if df._get_value(i,"monrecibi") == 1 or df._get_value(i,"monentre") ==1:
                    if df._get_value(i,"TipoOp")=="C":
                        df._set_value(i,"TipoOp","V")
                    else:
                        df._set_value(i, "TipoOp", "C")
                df._set_value(i, "plazo", (df._get_value(i, "fchvto") - df._get_value(i, "fchini")))
                df._set_value(i, "num_paist", self.ObtPais(df._get_value(i, "NroPai")))

        except Exception as e:
            return

vfetch = datetime.today()
skiprows =2
inpu=Coef5307(vfetch,skiprows)
inpu.run()



