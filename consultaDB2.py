#!/usr/bin/python3
# -*- coding: utf-8 -*-
# - consultaDB2.py -------------------------------------------------- #
# ------------------------------------------------------------------- #
# Author: Daniel Noronha da Silva C012743                             #
#  Email: danielns.py@gmail.com                                       #
# ------------------------------------------------------------------- #

# - Imports --------------------------------------------------------- #
from operator import index
from dotenv import load_dotenv
from os import getenv

import ibm_db
import ibm_db_dbi
import pandas as pd
# ------------------------------------------------------------------- #

# - Carregando variáveis de ambiente -------------------------------- #
load_dotenv()
# ------------------------------------------------------------------- #

# - Variáveis ------------------------------------------------------- #
DATABASE = getenv("DATABASE")
SERVER = getenv("SERVER")
UID = getenv("UID")
PWD_UID = getenv("PWD_UID")
ESQUEMA = getenv("ESQUEMA")
TABELA = getenv("TABELA")

# ------------------------------------------------------------------- #
# - Classes --------------------------------------------------------- #
class ConsultaDB2:
  def __init__(self):
    self.conexao_string = f"\
      database={DATABASE};\
      hostname={SERVER};\
      port=50000;\
      protocol=tcpip;\
      uid={UID};\
      pwd={PWD_UID}"

  def realizar_cosulta(self, select):
    # realizando conexão com a base do DB2
    ibm_db_conn = ibm_db.connect(self.conexao_string,"","")
    conexao = ibm_db_dbi.Connection(ibm_db_conn)

    # conecta com o esquema
    conexao.tables(ESQUEMA, TABELA)

    #cur = conexao.cursor()
    #cur.execute(select)
    df = pd.read_sql(select, conexao)
    #dados = pd.DataFrame.from_records(df)
    dados = df

    # desconectando do banco
    ibm_db.close(ibm_db_conn)

    #return dados.to_dict()
    return dados

  def query_um():
    select = ""
    return select

  def query_dois():
    select = "
    return select

 
# ------------------------------------------------------------------- #
