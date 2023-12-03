# import os
import json
import time
import requests
import numpy as np
# import pandas as pd
from art import *
from tqdm import tqdm
from time import sleep
import geopandas as gpd
from distutils.dir_util import mkpath
from requests.exceptions import Timeout
from concurrent.futures import ThreadPoolExecutor

print(text2art("IBGE API"))


def __save_file(
    content,
    filename,
    format='.csv',
    file_path='./ibge/'
):
    """This function save the file in the specified format.

    Args:
        content (dict): The content to be saved.
        filename (str): The name of the file.
        format (str, optional): The format of the file. Defaults to '.csv'.
    """
    
    mkpath(file_path)

    if format == '.csv':
        df = gpd.GeoDataFrame.from_features(content)
        df.to_csv(f'{file_path}{filename}{format}', index=False)
    elif format == '.geojson':
        with open(f'{file_path}{filename}{format}', 'w') as f:
            json.dump(content, f)
    elif format == '.shp':
        df = gpd.GeoDataFrame.from_features(content)
        df.to_file(f'{file_path}{filename}{format}')
    elif format == '.json':
        with open(f'{file_path}{UF}_final{format}', 'w') as f:
            json.dump(content, f)
    else:
        print('Formato não reconhecido')


def __get_metadados_municipios(id, content_dict, position):
    """_summary_

    Args:
        id (_type_): _description_
        content_dict (_type_): _description_
        position (_type_): _description_
    """

    # print(f'https://servicodados.ibge.gov.br/api/v3/malhas/municipios/{id}/metadados')
    while True:
        try:
            response = requests.get(
                url=f'https://servicodados.ibge.gov.br/api/v3/malhas/municipios/{id}/metadados',
                # params={
                #     'orderBy': orderBy,
                #     'view': view
                # },
                verify=True,
                timeout=30.00
            )
        except Timeout:
            continue
        code = response.status_code

        if code == 200:
            break
        elif code == 504:
            t = abs(10+np.random.randn())
        else:
            t = abs(60*(5 + np.random.randn()))
        print(f'Status code: {code}')
        print(f'Tentando novamente em {t:.0f} segundos')
        time.sleep(t)
    
    while True:
        try:
            response2 = requests.get(
                url=f'https://servicodados.ibge.gov.br/api/v1/localidades/municipios/{id}/distritos',
                # params={
                #     'orderBy': orderBy,
                #     'view': view
                # },
                verify=True,
                timeout=30.00
            )
        except Timeout:
            continue
        code2 = response2.status_code

        if code2 == 200:
            break
        elif code2 == 504:
            t = abs(10+np.random.randn())
        else:
            t = abs(60*(5 + np.random.randn()))
        print(f'Status code2: {code2}')
        print(f'Tentando novamente em {t:.0f} segundos')
        time.sleep(t)
    
    while True:
        try:
            response3 = requests.get(
                url=f'https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/2021/variaveis/9324?localidades=N6[{id}]',
                verify=True,
                timeout=30.00
            )
        except Timeout:
            continue
        code3 = response3.status_code

        if code3 == 200:
            break
        elif code == 504:
            t = abs(10+np.random.randn())
        else:
            t = abs(60*(5 + np.random.randn()))
        print(f'Status code: {code3}')
        print(f'Tentando novamente em {t:.0f} segundos')
        time.sleep(t)
    
    content = json.loads(response.content.decode('utf-8'))
    content_dict['features'][position]['properties']['centroide'] = list(content[0]['centroide'].values())
    content_dict['features'][position]['properties']['area'] = float(content[0]['area']['dimensao'])

    content2 = json.loads(response2.content.decode('utf-8'))
    content_dict['features'][position]['properties']['nome'] = content2[0]['nome']
    content_dict['features'][position]['properties']['id'] = content2[0]['municipio']['id']
    content_dict['features'][position]['properties']['microrregiao'] = content2[0]['municipio']['microrregiao']['nome']
    content_dict['features'][position]['properties']['mesorregiao'] = content2[0]['municipio']['microrregiao']['mesorregiao']['nome']
    content_dict['features'][position]['properties']['regiao_imediata'] = content2[0]['municipio']['regiao-imediata']['nome']
    content_dict['features'][position]['properties']['regiao_intermediaria'] = content2[0]['municipio']['regiao-imediata']['regiao-intermediaria']['nome']

    content3 = json.loads(response3.content.decode('utf-8'))
    content_dict['features'][position]['properties']['Pop_residente_estimada'] = float(content3[0]['resultados'][0]['series'][0]['serie']['2021'])


def api_IGBE(
        UF='PB',
        API_KEY='metadados',
        orderBy='nome',
        view='json',
        formato='application/vnd.geo+json',
        intrarregiao='municipio',
        file_path='./ibge/'
    ):
    """_summary_

    Args:
        UF (str, optional): _description_. Defaults to 'PB'.
        API_KEY (str, optional): _description_. Defaults to 'metadados'.
        orderBy (str, optional): _description_. Defaults to 'nome'.
        view (str, optional): _description_. Defaults to 'json'.
        formato (str, optional): _description_. Defaults to 'application/vnd.geo+json'.
        intrarregiao (str, optional): _description_. Defaults to 'municipio'.
    """

    if API_KEY == 'distritos':
        while True:
            try:
                response = requests.get(
                    url=f'https://servicodados.ibge.gov.br/api/v1/localidades/estados/{UF}/distritos',
                    params={
                        'orderBy': orderBy,
                        'view': view
                    },
                    verify=True,
                    timeout=30.00
                )
            except Timeout:
                continue
            code = response.status_code

            if code == 200:
                break
            elif code == 504:
                t = abs(10+np.random.randn())
            else:
                t = abs(60*(5 + np.random.randn()))
            print(f'Status code: {code}')
            print(f'Tentando novamente em {t:.0f} segundos')
            time.sleep(t)
            
        content = json.loads(response.content.decode('utf-8'))
        # print(content)

        mkpath(file_path)
        with open(f'{file_path}{UF}_distritos.json', 'w') as f:
            json.dump(content, f)
        # df = pd.DataFrame.from_records(content['properties']['parameter'])
        # print(df.head())

    # elif API_KEY == 'metadados':

    
    elif API_KEY == 'malhas':
        while True:
            try:
                response = requests.get(
                    url=f'https://servicodados.ibge.gov.br/api/v3/malhas/estados/{UF}',
                    params={
                        'formato': formato,
                        'intrarregiao': intrarregiao,
                        'qualidade': 'máxima'
                    },
                    verify=True,
                    timeout=30.00
                )
            except Timeout:
                continue
            code = response.status_code

            if code == 200:
                break
            elif code == 504:
                t = abs(10+np.random.randn())
            else:
                t = abs(60*(5 + np.random.randn()))
            print(f'Status code: {code}')
            print(f'Tentando novamente em {t:.0f} segundos')
            time.sleep(t)
        
        content = json.loads(response.content.decode('utf-8'))

        df = gpd.GeoDataFrame.from_features(content)

        with ThreadPoolExecutor(5) as pool:
            
            pbar = tqdm(total=len(df.codarea), desc=f'Coletando informações dos municípios ({UF}): ')
            futures = list()
            for row in df.iterrows():
                futures.append(
                    (
                        row[0],
                        pool.submit(
                            __get_metadados_municipios,
                            id=row[1]['codarea'],
                            content_dict=content,
                            position=row[0]
                        )
                    )
                )

            for position, future in futures:
                while not future.done:
                    sleep(0.5)
                e = future.exception()
                if e:
                    pbar.write(str(e))
                    f = pool.submit(
                        __get_metadados_municipios,
                        id=df.at[position, 'codarea'],
                        content_dict=content,
                        position=position
                    )
                    futures.append((position, f))
                else:
                    pbar.update()
            pbar.close()
        
        for f in ['.shp', '.csv', '.json', '.geojson']:
            __save_file(
                content,
                f'{UF}_malhas',
                format=f,
                file_path=file_path
            )

        # __save_file(content, UF, format='.shp', file_path=file_path)
        # __save_file(content, UF, format='.csv', file_path=file_path)
        # __save_file(content, UF, format='.json', file_path=file_path)
        # __save_file(content, UF, format='.geojson', file_path=file_path)
    
    else:
        print('API_KEY não reconhecida')


if __name__ == '__main__':

    UFs = [
        'AC',
        'AL',
        'AP',
        'AM',
        'BA',
        'CE',
        'DF',
        'ES',
        'GO',
        'MA',
        'MS',
        'MT',
        'MG',
        'PA',
        'PB',
        'PR',
        'PE',
        'PI',
        'RJ',
        'RN',
        'RS',
        'RO',
        'RR',
        'SC',
        'SP',
        'SE',
        'TO',
    ]

    pbar = tqdm(total=len(UFs), desc='Coletando informações dos estados do Brasil: ')

    for UF in UFs:
        
        api_IGBE(UF, API_KEY='malhas')
        pbar.update()

    pbar.close()