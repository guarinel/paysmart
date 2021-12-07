from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
import pandas as pd
from datetime import datetime


if __name__ == '__main__':

    es = Elasticsearch(
        [
            'https://elastic:iJwuxmFkJDFm2zoeeEaUpwSb@433e4a7f73fd4ed0a7436f398beaf0bd.sa-east-1.aws.found.io:9243',
        ],
        verify_certs=True, 
        headers = { "Content-Type": "application/json" })

    def get_api_acesso(mes, dia_inicial, dia_final):
        search_param = {
            "size": 100000,
            "_source": {
                "includes": [ "category", 'issuer', 'startdatetime', 'finishdatetime']},
            "query": {

                "range": { 
                    "startdatetime": {
                        "gte":'2021-{}-{}'.format(mes, dia_inicial), 
                        "lte":'2021-{}-{}'.format(mes, dia_final),
                        "relation" : "within" 
                    }
                }
            }
        }
        
        df_ = {}
        es_index = 'processadora_acesso_apis_mongodb_prod'
        res = helpers.scan(
                        client = es,
                        scroll = '15m',
                        query = search_param, 
                        index = es_index)
        A = list(res)
        
        dict_of_new_api_acess = {}
        for pos, item in enumerate(A):
            dict_of_new_api_acess[pos] = item['_source']
            
        df = pd.DataFrame.from_dict(dict_of_new_api_acess, orient='index')
        
        df['unique_id'] = df['finishdatetime'] + df['startdatetime']
        df = df.drop_duplicates(subset = 'unique_id')
        df['startDatetime'] = df['startdatetime'].apply(lambda x: x.split('T')[0])
        df['startDatetime']= df['startDatetime'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
        
        df_grouped = df.groupby(['issuer', 'category']).count().reset_index()
        df_final_apis = df_grouped.pivot(index='issuer', columns='category', values='startDatetime').fillna(0)
        df_final_apis.rename({'ACCOUNT_CANCEL': 'CANCELAMENTO DE CONTA', 'ACCOUNT_CREATION': 'CRIAÇÕES DE CONTA', 'ACCOUNT_UPDATE': 'ATUALIZAÇÃO DE CADASTRO', 'BLOCK_UNBLOCK_CARD': 'BLOQUEIO / DESBLOQUEIO', 
                      'CARDHOLDER_ASSIGNMENT': 'ASSOCIAÇÃO DE PORTADOR', 'CARD_CREATION': 'CRIAÇÕES DE CARTÃO', 'CARD_REISSUING': 'REEMISSÃO', 'CREATE_OR_UPDATE_DISPUTE': 'CRIAÇÃO DE DISPUTAS', 
                       'DISPUTES_QUERY': 'CONSULTA A DISPUTAS', 'MATTRESS_QUERY': 'CONSULTA AO COLCHÃO', 'OTHER': 'OUTRAS', 'PIN_CHANGE': 'TROCA DE PIN', 'SEARCH_OR_STATUS_QUERY': 'BUSCA OU CONSULTA', 'TRANSACTIONS_LIST': 'LISTAGEM DE TRANSAÇÕES'}, axis =1 , inplace = True)
        
    #     df_final_apis.rename({'ACCOUNT_CANCEL': 'CANCELAMENTO DE CONTA', 'ACCOUNT_CREATION': 'CRIAÇÕES DE CONTA', 'ACCOUNT_UPDATE': 'ATUALIZAÇÃO DE CADASTRO', 'BLOCK_UNBLOCK_CARD': 'BLOQUEIO / DESBLOQUEIO', 
    #                       'CARDHOLDER_ASSIGNMENT': 'ASSOCIAÇÃO DE PORTADOR', 'CARD_CREATION': 'CRIAÇÕES DE CARTÃO', 'CARD_REISSUING': 'REEMISSÃO', 'CREATE_OR_UPDATE_DISPUTE': 'CRIAÇÃO DE DISPUTAS', 
    #                        'DISPUTES_QUERY': 'CONSULTA A DISPUTAS', 'MATTRESS_QUERY': 'CONSULTA AO COLCHÃO', 'OTHER': 'OUTRAS', 'PIN_CHANGE': 'TROCA DE PIN', 'SEARCH_OR_STATUS_QUERY': 'BUSCA OU CONSULTA', 'TRANSACTIONS_LIST': 'LISTAGEM DE TRANSAÇÕES'}, axis =1 , inplace = True)
        df_final_apis.to_csv('final_apis_csv')
        return df_final_apis
    
    df_final_apis = get_api_acesso('11', '01', '30')