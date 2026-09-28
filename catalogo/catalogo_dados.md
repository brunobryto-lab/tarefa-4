# Catálogo de Dados

## Linhagem geral

`ANP ZIP/CSV -> tarefa_4_bronze_precos -> tarefa_4_silver_precos -> dimensões/fato e tabelas analíticas Gold`

Fonte oficial: Série Histórica de Preços de Combustíveis e de GLP da ANP. Extração realizada em 28/09/2026. As URLs e o período do arquivo são registrados na Bronze. Transformações completas estão no notebook `notebooks/01_pipeline_anp.py`.

## Camada Bronze - `tarefa_4_bronze_precos`

Todos os campos do CSV são texto para preservar o valor recebido. Além dos campos abaixo, a tabela contém `periodo_arquivo`, `url_origem` e `ingerido_em`.

| Atributo de origem | Tipo | Descrição | Domínio / nulabilidade | Linhagem |
|---|---|---|---|---|
| Regiao - Sigla | string | Região do posto | N, NE, CO, SE, S; obrigatório | ANP, sem transformação |
| Estado - Sigla | string | UF do posto | 27 UFs; obrigatório | ANP, sem transformação |
| Municipio | string | Município do posto | texto; obrigatório | ANP, sem transformação |
| Revenda | string | Razão social da revenda | texto | ANP; excluído da Silver por minimização |
| CNPJ da Revenda | string | Identificador da revenda | CNPJ | ANP; transformado em SHA-256 |
| Nome da Rua | string | Logradouro | texto | ANP; excluído da Silver |
| Numero Rua | string | Número | texto | ANP; excluído da Silver |
| Complemento | string | Complemento do endereço | texto; nulo permitido | ANP; excluído da Silver |
| Bairro | string | Bairro | texto; nulo permitido | ANP; excluído da Silver |
| Cep | string | CEP | texto | ANP; excluído da Silver |
| Produto | string | Combustível pesquisado | categorias da ANP | ANP; padronizado e filtrado |
| Data da Coleta | string | Data da observação | dd/MM/aaaa | ANP; convertido para date |
| Valor de Venda | string | Preço ao consumidor | decimal com vírgula | ANP; convertido para double |
| Valor de Compra | string | Preço de aquisição pelo posto | decimal; nulo permitido | ANP; convertido para double |
| Unidade de Medida | string | Unidade do preço | R$ / LITRO | ANP; padronizado |
| Bandeira | string | Bandeira declarada | categorias da ANP | ANP; padronizado |
| periodo_arquivo | string | Semestre do arquivo | 2024-01 a 2025-02 | derivado do nome/URL |
| url_origem | string | URL de download | URL HTTPS da ANP | acrescentado na ingestão |
| ingerido_em | timestamp | Horário da ingestão | timestamp; obrigatório | relógio do Databricks |

## Camada Silver - `tarefa_4_silver_precos`

| Atributo | Tipo | Descrição | Domínio / nulabilidade | Linhagem e transformação |
|---|---|---|---|---|
| regiao | string | Região do posto | N, NE, CO, SE, S; não nulo | maiúsculas e trim da Bronze |
| uf | string | Unidade da Federação | duas letras; não nulo | maiúsculas e trim da Bronze |
| municipio | string | Município | texto não vazio | maiúsculas e trim da Bronze |
| posto_id | string | Identificador pseudonimizado | SHA-256; não nulo | hash do CNPJ |
| produto | string | Combustível | GASOLINA ou ETANOL | padronizado, sem acentos e filtrado |
| data_coleta | date | Data do preço | 01/01/2024 a 31/12/2025 | conversão de dd/MM/aaaa |
| valor_venda | double | Preço ao consumidor | R$ 0,50 a R$ 20,00/l; não nulo | vírgula substituída por ponto e cast |
| valor_compra | double | Preço de aquisição | R$ 0,50 a R$ 20,00/l; nulo permitido | cast; 100% ausente neste recorte |
| unidade_medida | string | Unidade | R$ / LITRO | maiúsculas e trim |
| bandeira | string | Bandeira do posto | categoria ANP; nulo permitido | maiúsculas e trim |
| periodo_arquivo | string | Semestre de origem | 2024-01 a 2025-02 | herdado da Bronze |
| url_origem | string | URL de origem | HTTPS ANP | herdado da Bronze |
| ingerido_em | timestamp | Momento da ingestão | timestamp | herdado da Bronze |
| ano | int | Ano da coleta | 2024 ou 2025 | derivado de data_coleta |
| mes | string | Ano e mês | AAAA-MM | derivado de data_coleta |
| registro_valido | boolean | Aprovação das regras mínimas | true/false; não nulo | data, preço e UF conformes |

## Camada Gold - modelo estrela

### `tarefa_4_fato_precos`

| Atributo | Tipo | Descrição | Domínio / nulabilidade | Linhagem |
|---|---|---|---|---|
| posto_id | string | Chave do posto | hash; obrigatório | Silver |
| localidade_id | string | Chave da localidade | hash; obrigatório | região + UF + município |
| produto_id | string | Chave do produto | hash; obrigatório | produto + unidade |
| data_coleta | date | Chave de tempo | data válida; obrigatória | Silver |
| valor_venda | double | Medida de preço | 0,50 a 20,00 | Silver validada |
| valor_compra | double | Medida de custo | nulo permitido | Silver |
| periodo_arquivo | string | Partição lógica de origem | quatro semestres | Silver |

### Dimensões

| Tabela | Chave | Atributos | Regra |
|---|---|---|---|
| tarefa_4_dim_localidade | localidade_id | região, UF, município | combinações distintas da Silver |
| tarefa_4_dim_produto | produto_id | produto, unidade_medida | combinações distintas da Silver |
| tarefa_4_dim_tempo | data_coleta | ano, mês, ano_mes | datas distintas da Silver |
| tarefa_4_dim_posto | posto_id | bandeira | postos pseudonimizados distintos |

## Tabelas analíticas Gold

| Tabela | Grão | Finalidade |
|---|---|---|
| tarefa_4_qualidade | atributo | completude, cardinalidade, mínimos e máximos |
| tarefa_4_precos_uf_2025 | UF e produto | responder extremos estaduais em 2025 |
| tarefa_4_evolucao_mensal | mês e produto | medir tendência temporal |
| tarefa_4_competitividade_etanol | UF | medir meses com razão etanol/gasolina <= 0,70 |
| tarefa_4_dispersao | UF e produto | comparar coeficiente de variação e amplitude central |

