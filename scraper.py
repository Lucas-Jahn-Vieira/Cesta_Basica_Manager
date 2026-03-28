import scrapy
from scrapy.crawler import CrawlerProcess
import os
import re

def clear_previous_json():
        if os.path.exists('produtos.json'):
            os.remove('produtos.json')

#CONFIGURANDO O SCRAPPER ------------------------------------------------
class CestaBasicaSpider(scrapy.Spider):
    name = "cesta_basica"
    produto_volume = {'arroz':5, 'feijao':2, 'oleo_de_soja':0.9, 'acucar':1, 'cafe':0.5}
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

    #pega todas as sitelists de produtos
    def parse(self, response: scrapy.http.Response):
        response.selector.remove_namespaces()

        lista_sites = response.xpath('.//sitemap/loc[contains(text(), "/product")]/text()').getall()

        for site in lista_sites[:1]:
            nome_site = site.split("/")[-1]
            print(f'Visitando site: {nome_site}')
            if site != None:
                yield response.follow(site, self.parse_site)

    #parse dos itens dentro das sitelists
    def parse_site(self, response: scrapy.http.Response):
        response.selector.remove_namespaces()

        for produto in self.produto_volume.keys():
            lista_produtos = response.xpath(f'//url/loc[contains(text(), "/{produto}")]/text()').getall()

            for p in lista_produtos:
                if p != None:
                    site_produto = p
                    print(f'Visitando produto -> {site_produto}')
                    yield response.follow(p, self.get_product_info)
                else:
                    print(f'{produto} não foi encontrado')

    #pega nome, preço, peso, etc dos produtos encotrados
    def get_product_info(self, response: scrapy.http.Response):
        response.selector.remove_namespaces()

        nome = response.css('span.vtex-store-components-3-x-productBrand::text').get()
        preco_total = response.xpath('//meta[contains(@property, "product:price:amount")]/@content').get()
        
        if preco_total:
            peso_desejado = self.get_product_weight(response.url)
            preco_pesado = self.set_weight_to_price(response.url, preco_total, peso_desejado)

            print(f'{nome}: {preco_pesado}/{peso_desejado}kg|L R$')

            yield{
                "produto": nome,
                "preco_total": preco_pesado,
                "quantia": peso_desejado,
                "url_do_produto": response.url
            }
        else:
            print(f'{nome}: está indisponível')

            yield{
                "produto": nome,
                "preco_total": None,
                "url_do_produto": response.url
            }
    
    def get_product_weight(self, nome) -> str:
        extracao = re.search(r'(arroz|feijao|oleo_de_soja|acucar|cafe)', nome.lower())
        tipo = extracao.group(1)

        peso_desejado = self.produto_volume[tipo]
        return peso_desejado

    def set_weight_to_price(self, nome, overall_price, desired_weight) -> float:
        #extair valor do peso e unidade de medida
        extracao = re.search(r'(\d+)(kg|g|ml|l)', nome.lower())
        weight = float(extracao.group(1))
        medida = extracao.group(2)

        if medida == 'g' or medida == 'ml':
            weight = weight / 1000

        price_per_kg = float(overall_price) / weight

        total_price = round(price_per_kg * desired_weight, 2)
        return total_price
        
    
process = CrawlerProcess(settings={
    "FEEDS": {
        "produtos.json": {"format": "json"},
    },
})

def init_scraper():
    clear_previous_json()

    process.crawl(CestaBasicaSpider)
    process.start()

#init_scraper()

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