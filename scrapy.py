import scrapy
import w3lib.html
import requests
import pandas


#CONFIGURANDO O SCRAPPER ------------------------------------------------
class QuotesSpider(scrapy.Spider):
    name = "cesta_basica"
    start_urls = [
        #base de dados do gov, ipca
        "https://api.bcb.gov.br/dados/serie/bcdata.sgs.10844/dados?formato=json",
    ]
    custom_settings = {
        'DOWNLOAD_DELAY': 2, #média de 2 segundos de delay entre requisições
        'RANDOMIZE_DOWNLOAD_DELAY': True, # randomiza o delay
        'CONCURRENT_REQUESTS': 1, #num. de requests consecutivas (Aumente após validar a lógica de extraçaõ)
        'LOG_FILE': 'scrapy_output.log',
        'HTTPCACHE_ENABLED' : True, # Usamos cache para não gerar problemas para o site do Giassi
        'HTTPCACHE_EXPIRATION_SECS' : 86400, # 24 hours in seconds
        'HTTPCACHE_DIR' : 'cache',
        'HTTPCACHE_IGNORE_HTTP_CODES' : [404, 500, 502, 503]
    }

    @classmethod
    def update_settings(cls, settings):
        super().update_settings(settings)
        settings.set("BOT_NAME", "Pesquisa_Linhas_CFX", priority="spider")
    

    #"REAL" LÓGICA DO SCRAPPER

    #função que utiliza oq for encontrado ná página
    #response -> tudo o que foi encontrado na página
    def parse(self, response: scrapy.http.Response):
        
        response.selector.remove_namespaces() #retira arquivos xml para facilitar busca

        #variável de busca dentro do () busca tudo que tiver o texto (contains(text())) dado no 2° parâmetro
        #o /text() no final garante que não virá tags (<h1>, <loc>, etc) junto
        horarios = response.xpath('//url/loc[contains(text(),"/horarios/")]/text()')
        print('Horarios: ') #print simples
        for url in horarios: #itera pelos itens encontrados
            if url is not None: #caso o item não for nulo
                #yield adiciona uma ação a lista de ações a serem feitas enquanto o código corre
                #respons.follow(qual url seguir, função pra tratar a resposta dessa nova url)
                yield response.follow(url, self.parse_details)