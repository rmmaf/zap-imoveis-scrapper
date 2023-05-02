import json
import pandas as pd
import numpy as np
import math
from typing import Literal
from html2text import html2text

class EmptyResponseError(TypeError):
    def __init__(self, message="Response Vazia"):
        self.message = message
        super().__init__(self.message)

class KeyResponseError(KeyError):
    def __init__(self, message="Response com Chave incorreta"):
        self.message = message
        super().__init__(self.message)

def get_max_page_from_response(data_string) -> int:
    try:
        data = json.loads(data_string)
        total_count = int(data['search']['totalCount'])
    except KeyError:
        raise KeyResponseError
    except TypeError:
        raise EmptyResponseError
    pages = math.ceil(total_count/100)
    if pages >= 100:
        return 100
    else:
        return pages
    
def append_and_check(out_list: list, in_list, input:str, join_link:str=None, join_append:bool=False, html_text: bool=False) -> None:
    try:
        if join_link is not None:
            out_list.append(f"{join_link}{in_list[input]}")
        elif join_append:
            out_list.append(', '.join(map(str, in_list[input])))
        elif html_text:
            out_list.append(html2text(in_list[input]))
        else:    
            out_list.append(in_list[input])
    except KeyError:
        out_list.append(np.nan)

def read_response(data_string) -> pd.DataFrame:
    data = json.loads(data_string)
    base_link = "https://www.zapimoveis.com.br"

    area = []
    descricao = []
    nome = []
    tipo_unidade = []
    comodidades = []
    pais = []
    cep = []
    cidade = []
    rua = []
    estado = []
    suites = []
    banheiros = []
    vagas = []
    telefones = []
    whatsapp = []
    quartos = []
    preco = []
    iptu = []
    tipo_negocio = []
    periodo_aluguel = []
    garantia_aluguel = []
    preco_mensal_aluguel = []
    condominio = []
    vendedor = []
    link_vendedor = []
    link_imovel = []
    try:
        super_premium_data = data['superPremium']['search']['result']["listings"]
    except KeyError:
        super_premium_data = []
    listing_data = [*data['search']['result']["listings"], *super_premium_data]
    for i in listing_data:
        att_list = i['listing']

        append_and_check(area, att_list, 'usableAreas', join_append=True)
        append_and_check(tipo_unidade, att_list, 'unitTypes', join_append=True)
        append_and_check(comodidades, att_list, 'amenities', join_append=True)
        append_and_check(descricao, att_list, 'description', html_text=True)
        append_and_check(nome, att_list, 'title')

        endereco = att_list['address']

        append_and_check(pais, endereco, 'country')
        append_and_check(cep, endereco, 'zipCode')
        append_and_check(cidade, endereco, 'city')
        append_and_check(rua, endereco, 'street')
        append_and_check(estado, endereco, 'state')

        append_and_check(suites, att_list, 'suites', join_append=True)
        append_and_check(banheiros, att_list, 'bathrooms', join_append=True)

        append_and_check(vagas, att_list, 'parkingSpaces', join_append=True)
        append_and_check(quartos, att_list, 'bedrooms', join_append=True)
        append_and_check(whatsapp, att_list, 'whatsappNumber')

        add_ctt = att_list['advertiserContact']
        append_and_check(telefones, add_ctt, 'phones', join_append=True)

        custos = att_list["pricingInfos"][0]
        append_and_check(tipo_negocio, custos, 'businessType')
        try:
            append_and_check(periodo_aluguel, custos["rentalInfo"], 'period')
            append_and_check(garantia_aluguel, custos["rentalInfo"], "warranties", join_append=True)
            append_and_check(preco_mensal_aluguel, custos["rentalInfo"], "monthlyRentalTotalPrice")
        except KeyError:
            periodo_aluguel.append(np.nan)
            garantia_aluguel.append(np.nan)
            preco_mensal_aluguel.append(np.nan)
            
        append_and_check(preco, custos, 'price')
        append_and_check(iptu, custos, 'yearlyIptu')
        append_and_check(condominio, custos, 'monthlyCondoFee')
        
        info_vendor = i['account']
        append_and_check(vendedor, info_vendor, 'name')
        address_vendor = i['accountLink']
        append_and_check(link_vendedor, address_vendor, 'href', base_link)

        address_imovel = i['link']
        append_and_check(link_imovel, address_imovel, 'href', base_link)

    response_df = pd.DataFrame({"area":area, "descricao":descricao,
                                "nome":nome, "tipo_unidade":tipo_unidade, 
                                "comodidades":comodidades, "pais":pais, "cep":cep, "cidade":cidade,
                                "rua":rua, "estado":estado, "suites":suites, "banheiros":banheiros,
                                "telefones":telefones, "whatsapp":whatsapp,
                                "quartos":quartos, "vagas":vagas,
                                "tipo_negocio":tipo_negocio, "periodo_aluguel":periodo_aluguel,
                                "garantia_aluguel":garantia_aluguel, "preco_mensal_aluguel":preco_mensal_aluguel,
                                "preco":preco, "iptu":iptu, "condominio":condominio, "vendedor":vendedor,
                                "link_vendedor":link_vendedor, "link_imovel":link_imovel})
    return response_df
