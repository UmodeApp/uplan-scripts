
### Setup python project

```
pyenv local 3.10
python -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt
```

## Script para checagem de SKUs

Etapa 1
1.1 - Popular o banco com 10 itens/linhas (manualmente) de pedidos da colmeia em Themis.IncomingRawData.IncomingRawOrders
1.2 - Organizar o método que vai salvar os dados brutos do cliente diretamente em Themis.IncomingRawData.IncomingRawOrders
1.3 - Criar o método de tradução no esquema final de orders, para transpor os 10 itens de 1.1 --> para Incoming.Orders

Etapa 2
2.1 - Criar um método para checar se as 6500 SKUs da Colmeia estão no banco Themis.IncomingRawData.IncomingRawItems
2.2 - Organizar o método que vai salvar os dados brutos dos items do cliente diretamente em Themis.IncomingRawData.IncomingRawItems - (Edno checar com Andre)
2.3 - Criar o método de tradução no esquema final de items para transpor todos os itens de 2.1 --> para Incoming.Items
2.4 - Se 2.3 tiver sucesso, transpor o item para Atlas.Items.Items (Banco de Dados de Produção dos Items, após a Integração)

Etapa 3
3.1 - Organizar o método que vai salvar os dados brutos do estoque dos SKU diretamente em Themis.IncomingRawData.IncomingSKUStock - (Edno checar com Andre)
3.2 - Criar o método de tradução no esquema final de items para transpor todos os itens de 2.1 --> para <definir em conjunto o banco para estoque>

Etapa 4
4.1 - Criar um serviço offline para o Stage 1, fazer o "fetch data", checar a integridade dos itens, emitir relatório e <proceed>
4.2 - Iniciar a primeira etapa do desenho do fluxo dos cálculos para depois voltarmos com o pente fino nos dados

itemBrandSKUaRRAY: SKUS
PATTENERdATA
REFsKUS
REFSKU + SKUSPECIFICATION COR_NAME
2 02 04 1 0523 MARINHO

itemBrandSKUaRRAYinstoreSKU
REF + COR

### Entendendo
"live" - Objeto final no banco de dados Anubis -> Items -> Items
orders - COmputador em são paulo para rodar os mihloes de itens

-Os lotes são calculadosatravés dos dados de pedidos enquanto o schema de 
produtos são gerados atravéz das listas de produtos.

Agora: 
IcomeRawItems -> icominG - Items
Criar script para criar peças existentes e script para peças não existentes

#### Checagem de validação de produtos nos lotes

- Rodar o script de correlação de skus - OK
- Dizer quantos skus estão sem correlação
- Pegar do fábio, todos os itens do resultado do lote 2. - OK
- Lista de todas as referencias e chekar se estão no banco ou n.


- Quantos itens no banco ainda estão sem o campo itemBrandInstoreSkuArray: 3466
- Quantas REFS do novo lote estão no banco: 51
 


# OBS
- Os orders são criados atravez dos ordersRawData
- Itens simples são criados atravez dos orders, quando não se tem ItemRaw 
- Politicas de manter dados
- Executar script por id mongo que é um hash de date

orderSalesChanel = 1 | 2 # 1 - ecormercer 2 - Loja física
