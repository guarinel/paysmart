"""
Script para triggar tabelas no dynamodb para ativar a stream de dados anteriores a criação da stream.
O scrip irá atualizar um campo de acordo com a entrada do usuário e irá retornar para o valor original.
"""

import sys
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key


def select_profile():
    profiles = boto3.session.Session().available_profiles

    print("---\nSelecione o profile AWS disponível (caso não encontre o procurado deve ser inserido via AWS CLI):")
    for i, p in enumerate(profiles):
        print(f"{i}. {p}")

    global profile_selected

    try:
        profile_selected = profiles[0]
    except IndexError:
        print("Selecione um valor válido.")

    print(f"[Profile selecionado: {profile_selected}]")
    return profile_selected


if __name__ == '__main__':
    print("\nTrigger placebo de tabelas dynamodb.")
    profile_selected = select_profile()
    session = boto3.session.Session(profile_name=profile_selected)
    region_name = session.region_name
    print(f"[Região:{region_name}]")
    print(f"[Perfil: {profile_selected}]")
    dynamodb = session.resource("dynamodb", endpoint_url=f"https://dynamodb.{region_name}.amazonaws.com/")

    # print("Nome da tabela:")
    table_names = ['transaction_service_ifood_transactions',  'transaction_service_beevale_transactions', 'transaction_service_akar_transactions', 
                   'transaction_service_celer_transactions', 'transaction_service_ramosesilva_transactions', 
                   'transaction_service_selfpay_transactions', 'transaction_service_kardbank_transactions', 'transaction_service_brasilcash_transactions', 'transaction_service_asaas_transactions', 
                   'transaction_service_pagme_transactions', 'transaction_service_seuvale_transactions', 'transaction_service_sulcredi_transactions',
                   'transaction_service_qitech_transactions', 'transaction_service_volus_transactions', 'transaction_service_picpay_transactions', 'transaction_service_batpay_transactions',
                   'transaction_service_allbank_transactions', 'transaction_service_trampolin_transactions', 'transaction_service_aixmobil_transactions', 'transaction_service_fitbank_transactions',
                   'transaction_service_leebank_transactions', 'transaction_service_dizconta_transactions', 'transaction_service_contapronta_transactions',
                   'transaction_service_brm1_transactions', 'transaction_service_issuer_transactions', 'transaction_service_livre_transactions', 'transaction_service_moneypag_transactions',
                   'transaction_service_personalplus_transactions', 'transaction_service_atar_transactions', 'transaction_service_pagprime_transactions',
                    'transaction_service_t10_transactions',  'transaction_service_supera_transactions', 'transaction_service_ewally_transactions',
                   'transaction_service_qgx_transactions', 'transaction_service_livelo_transactions', 'transaction_service_adiq_transactions',
                   'transaction_service_glorissuer_transactions', 'transaction_service_cashin_transactions', 'transaction_service_aixclubes_transactions'] 
    for table_name in table_names:        
        # table_name = input()
        table = dynamodb.Table(table_name)

        described = None
        try:
            described = session.client(
                "dynamodb",
                endpoint_url=f"https://dynamodb.{region_name}.amazonaws.com/")\
                .describe_table(TableName=table_name)
        except ClientError as error:
            if error.response['Error']['Code'] == 'ResourceExceptionNotFound':
                print("Erro: tabela não existe")
                continue
                # sys.exit()
            else:
                print(f"Erro {error.response['Error']['Message']}")
                sys.exit()

        print(f"Descrição da tabela: {described}")
        pk = None
        sk = None

        for key in described['Table']['KeySchema']:
            if key['KeyType'] == "HASH":
                pk = key['AttributeName']
            elif key['KeyType'] == "RANGE":
                sk = key['AttributeName']

        print(f"Coluna primária da tabela: {pk}")
        if sk is not None:
            print(f"Coluna secundária da tabela: {sk}")

        print("Chave que será utilizada para atualização placebo:")
        update_key = 'psProductName'

        fe = Key('paymentDate').between('2021-11-19','2021-11-25');
        response = table.scan(
                FilterExpression=fe
            )

        #response = table.scan()
        data = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])

        print(f"Exemplo de registro: {data[0]}")
        update_expression = f'SET {update_key} = :{update_key}'
        print(f"Update Expression:\n{update_expression}")

        print("Iniciar processo: 1-SIM 2-NÃO")
        init = 1
        if init == 2:
            sys.exit()

        try:
            for idx, item in enumerate(data):
                if pd.to_datetime(item['paymentDate']) >= pd.to_datetime('2021-11-20'):
                    print(f"({idx}/{len(data)}):")
                    original_expression_attribute = {
                        f':{update_key}': item[f'{update_key}']
                    }

                    expression_attribute = {
                        f':{update_key}': item[f'{update_key}'] + "-placebo"
                    }
                    print(f"Expression attributes applied: {expression_attribute}")
                    keys = {
                        f'{pk}': item[f'{pk}']
                    }

                    if sk is not None:
                        keys['sk'] = item[f'{sk}']

                    print(f"Chaves de classificação: {keys}")
                    response = table.update_item(
                        Key={
                            f'{pk}': item[f'{pk}'],
                            f'{sk}': item[f'{sk}']
                        },
                        UpdateExpression=update_expression,
                        ExpressionAttributeValues=expression_attribute,
                        ReturnValues="UPDATED_NEW"
                    )
                    print(f"Resposta de trigger 'PLACEBO': {response['Attributes']}")

                    response = table.update_item(
                        Key={
                            f'{pk}': item[f'{pk}'],
                            f'{sk}': item[f'{sk}']
                        },
                        UpdateExpression=update_expression,
                        ExpressionAttributeValues=original_expression_attribute,
                        ReturnValues="UPDATED_NEW"
                    )
                    print(f"Resposta de ROLLBACK: {response['Attributes']}")

        except (Exception, ClientError) as error:
            if 'response' in error and error.response['Error']['Code'] == 'ValidationException':
                print(f"Erro: {error.response['Error']['Message']}")
                sys.exit()
            else:
                raise
