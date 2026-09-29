# Índice de evidências

| Arquivo | Evidência | Seção do README |
|---|---|---|
| `top_fornecedores.png` | concentração de CVEs explorados por fornecedor | 7.1 |
| `evolucao_anual_kev.png` | inclusões anuais no catálogo CISA KEV | 7.2 |
| `severidade_ransomware.png` | distribuição CVSS e associação conhecida a ransomware | 7.3 |
| `qualidade_por_atributo.csv` | completude, tipos, cardinalidade, mínimos e máximos | 6 |
| `execucao_databricks.md` | execução Serverless e contagens das 14 tabelas Delta persistidas | 8 e 9 |
| `databricks_execucao_concluida.png` | captura do notebook concluído, resumo da integração e auditoria por atributo | 6, 8 e 9 |
| `databricks_tabelas_persistidas.png` | captura da saída final com 14 tabelas Delta, contagens e tempo de execução | 8 e 9 |
| `auditoria_final.md` | conferência do projeto contra o checklist e os critérios do enunciado | 10 |

As imagens foram geradas a partir dos mesmos JSONs oficiais processados pelo notebook. Os arquivos brutos não são versionados, pois o pipeline os coleta diretamente da CISA e do NVD e preserva URL, versão e horário de ingestão na camada Bronze.
