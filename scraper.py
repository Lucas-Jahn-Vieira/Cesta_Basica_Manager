import scrapy
from scrapy.crawler import CrawlerProcess
import w3lib.html
import requests
import pandas


#CONFIGURANDO O SCRAPPER ------------------------------------------------
class CestaBasicaSpider(scrapy.Spider):
    name = "cesta_basica"
    start_urls = [
        #lista de urls do site do giassi
        "https://www.giassi.com.br/sitemap.xml",
    ]
    custom_settings = {
        'DOWNLOAD_DELAY': 2, #média de 2 segundos de delay entre requisições
        'RANDOMIZE_DOWNLOAD_DELAY': True, # randomiza o delay
        'CONCURRENT_REQUESTS': 1, #num. de requests consecutivas (Aumente após validar a lógica de extraçaõ)
        'LOG_FILE': 'scrapy_output.log',
        'LOG_LEVEL': 'ERROR', #toque do Jahn para ver os prints ;)
        'HTTPCACHE_ENABLED' : True, # Usamos cache para não gerar problemas para o site do Giassi
        'HTTPCACHE_EXPIRATION_SECS' : 86400, # 24 hours in seconds
        'HTTPCACHE_DIR' : 'cache',
        'HTTPCACHE_IGNORE_HTTP_CODES' : [404, 500, 502, 503]
    }

    @classmethod
    def update_settings(cls, settings):
        super().update_settings(settings)
        settings.set("BOT_NAME", "Pesquisa_Linhas_CFX", priority="spider")
    

    #"REAL" LÓGICA DO SCRAPPER -----------------------------------------------------------------------

    def parse(self, response: scrapy.http.Response):
        response.selector.remove_namespaces()

        lista_sites = response.xpath('.//sitemap/loc[contains(text(), "/product")]/text()').getall()

        for site in lista_sites[:3]:
            nome_site = site.split("/")[-1]
            print(f'Visitando site: {nome_site}')
            if site != None:
                yield response.follow(site, self.parse_site)

    def parse_site(self, response: scrapy.http.Response):
        response.selector.remove_namespaces()

        products_to_search = ['arroz', 'feijão']
        for product in products_to_search:
            product_list = response.xpath(f'//url/loc[contains(text(), "/{product}")]/text()').getall()

            for p in product_list:
                if p != None:
                    nome_produto = p#.split('/')[-2]
                    print(f'Visitando produto -> {nome_produto}')
                    yield response.follow(p, self.get_product_info)

    def get_product_info(self, response: scrapy.http.Response):
        response.selector.remove_namespaces()

        nome = response.css('span.vtex-store-components-3-x-productBrand::text').get()
        preco = response.xpath('//meta[contains(@property, "product:price:amount")]/@content').get()

        produtos_ind = []
        if preco:
            print(f'{nome}: {preco}R$')
        else:
            print(f'{nome} está indisponível')
    
process = CrawlerProcess(settings={
    "FEEDS": {
        "saida.json": {"format": "json"},
    },
})
process.crawl(CestaBasicaSpider)
process.start()


"""
-- função que utiliza oq for encontrado ná página
-- response -> tudo o que foi encontrado na página
def parse(self, response: scrapy.http.Response):
    
    response.selector.remove_namespaces() -- retira palavras como 'xml' ou 'https:' para facilitar busca

    -- busca é feita pelas tags, nesse exemplo -> todas as tagas loc, dentro de uma tag url
    -- variável de busca dentro do () busca tudo que tiver o texto (contains(text())) dado no 2° parâmetro
    -- o /text() no final garante que não virá tags (<h1>, <loc>, etc) junto
    horarios = response.xpath('//url/loc[contains(text(),"/horarios/")]/text()')
    print('Horarios: ') -- print simples
    for url in horarios: -- itera pelos itens encontrados
        if url is not None: -- caso o item não for nulo
            -- yield adiciona uma ação a lista de ações a serem feitas enquanto o código corre
            -- respons.follow(qual url seguir, função pra tratar a resposta dessa nova url)
            yield response.follow(url, self.parse_details)"""