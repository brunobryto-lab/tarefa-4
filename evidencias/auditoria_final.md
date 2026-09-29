# Auditoria final da Tarefa 4

Auditoria realizada em 29/09/2026 contra os requisitos e o checklist do enunciado do MVP da disciplina.

## Checklist de entrega

| Exigência | Situação | Evidência |
|---|---|---|
| Objetivo e perguntas de negócio documentados | Atendido | README, seções 1 e 7; cinco perguntas preservadas e respondidas |
| Origem, licença e data de coleta registradas | Atendido | README, seção 2; endpoints oficiais CISA e NVD, coleta de 28/09/2026 e licença MIT para código e documentação |
| Dados persistidos na nuvem | Atendido | `execucao_databricks.md` e `databricks_tabelas_persistidas.png`; 14 tabelas Delta em `workspace.default` |
| Modelo de dados, domínios e linhagem | Atendido | `catalogo/catalogo_dados.md` e `catalogo/modelo_estrela.mmd` |
| Transformações, carga e regras de negócio documentadas | Atendido | README, seções 3 a 5, e notebook reproduzível |
| Qualidade para os atributos relevantes | Atendido | README, seção 6; `qualidade_por_atributo.csv`; tabela `tarefa_4_qualidade` com 27 linhas |
| Perguntas respondidas com evidência e discussão | Atendido | README, seções 7.1 a 7.5; gráficos e arquivos em `resultados/` |
| Discussão geral conectada ao problema | Atendido | README, seção 7.5 |
| Autoavaliação, dificuldades e trabalhos futuros | Atendido | README, seção 10 |
| Capturas e resultados legíveis e referenciados | Atendido | duas capturas do Databricks na seção 9 e três gráficos nas seções 7.1 a 7.3 |
| Repositório público | Atendido | `https://github.com/brunobryto-lab/tarefa-4`, resposta HTTP 200 sem autenticação em 29/09/2026 |

## Conferência pelos critérios de avaliação

| Critério | Evidência de atendimento |
|---|---|
| Objetivo (1,0) | Problema, hipótese e cinco perguntas específicas ligadas à decisão de priorização |
| Coleta (0,5) | Duas fontes oficiais, endpoints, data, volume, formato JSON e persistência Bronze documentados |
| Modelagem (2,0) | Esquema estrela com fato, dimensões, ponte CVE-CWE, catálogo de dados, domínios e linhagem |
| Carga (1,0) | Pipeline ETL executado ponta a ponta, escritas Delta idempotentes e 14 tabelas relidas após a gravação |
| Análise (3,0) | Auditoria por atributo, cinco respostas técnicas e discussão crítica das limitações |
| Autoavaliação (0,5) | Objetivos atingidos, dificuldades, limitações e extensões para uso contínuo |
| Capricho (2,0) | Repositório organizado, README navegável, gráficos, capturas, resultados auditáveis e reprodução documentada |

## Resultado técnico final

- Run all concluído no Databricks Serverless em 4 min 02 s.
- 1.728 registros CISA, 1.728 registros NVD, 1.728 integrados e 1.728 válidos.
- CVSS entre 2,7 e 10,0.
- Listas CWE vazias tratadas como ausência na auditoria.
- 14 tabelas Delta persistidas no catálogo `workspace.default`.
- Repositório local sem alterações pendentes após a publicação final.

O projeto atende aos requisitos técnicos e documentais do enunciado. A postagem no ambiente virtual da disciplina é uma etapa externa ao repositório.
