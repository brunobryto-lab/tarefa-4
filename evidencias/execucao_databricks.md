# Registro da execução no Databricks

- Ambiente: Databricks Free Edition, computação Serverless.
- Notebook: `tarefa-4-pipeline-anp`.
- Catálogo e esquema: `workspace.default`.
- Execução concluída com sucesso em 28/09/2026, às 20:11 (America/Sao_Paulo).
- Duração indicada pelo notebook: 2 min 53 s.
- Fonte executada: `notebooks/01_pipeline_anp.py`, revisão `b0485e7` do repositório público.

## Contagens verificadas na execução

| Tabela Delta | Linhas persistidas |
|---|---:|
| `tarefa_4_bronze_precos` | 1.712.267 |
| `tarefa_4_silver_precos` | 820.498 |
| `tarefa_4_dim_localidade` | 462 |
| `tarefa_4_dim_produto` | 2 |
| `tarefa_4_dim_tempo` | 544 |
| `tarefa_4_dim_posto` | 19.696 |
| `tarefa_4_fato_precos` | 820.498 |
| `tarefa_4_qualidade` | 16 |
| `tarefa_4_precos_uf_2025` | 54 |
| `tarefa_4_evolucao_mensal` | 48 |
| `tarefa_4_competitividade_etanol` | 27 |
| `tarefa_4_dispersao` | 54 |

## Verificações observadas

- status final da célula: **succeeded**;
- leitura final das 12 tabelas após a gravação, comprovando persistência;
- qualidade calculada para 16 atributos da Silver;
- 27 UFs e 461 municípios nos dados analíticos;
- intervalo de datas de 01/01/2024 a 31/12/2025;
- `valor_compra` com 820.498 nulos e `valor_venda` sem nulos, de R$ 2,63 a R$ 9,29.

As saídas completas continuam associadas ao notebook salvo no workspace do autor. Não foi publicada captura da barra de navegação porque ela contém o endereço de e-mail da conta.
