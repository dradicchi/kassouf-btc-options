from itertools import islice

# Converter o cursor em uma lista para poder iterar com islice
documentos = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,]
documentos.reverse()

# Função para obter janelas móveis de 11 dias
def obter_janelas(lista, tamanho_janela):
    for i in range(len(lista) - tamanho_janela + 1):
        yield lista[i:i + tamanho_janela]

# Iterar pelas janelas móveis de 11 dias
for janela in obter_janelas(documentos, 3):
    # Cada 'janela' é uma lista de 11 documentos (dias)
    print("Janela de 11 dias:")
    for doc in janela:
        print(doc)
    print("--- Avançando para a próxima janela ---")