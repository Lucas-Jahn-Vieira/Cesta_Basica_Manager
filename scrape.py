import scrapy
import w3lib.html
import requests
import pandas

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
    
    def parse(self, response: scrapy.http.Response):
        
        response.selector.remove_namespaces()
        horarios = response.xpath('//url/loc[contains(text(),"/horarios/")]/text()')
        print('Horarios: ')
        for url in horarios:
            if url is not None:
                yield response.follow(url, self.parse_details)
