# Catálogo de dados — CISA KEV + NVD

## Domínio e linhagem

O domínio é **gestão de vulnerabilidades em segurança cibernética**. O grão da tabela fato é uma vulnerabilidade identificada por CVE e confirmada pela CISA como explorada no mundo real.

```text
CISA KEV JSON ──> Bronze CISA ──┐
                                ├──> Silver integrada ──> dimensões, fato e ponte CVE–CWE ──> agregações Gold
NVD CVE API ────> Bronze NVD ───┘
```

| Camada | Tabela | Grão | Finalidade |
|---|---|---|---|
| Bronze | `tarefa_4_bronze_cisa_kev` | uma vulnerabilidade do catálogo CISA | preservar atributos, metadados e URL da fonte |
| Bronze | `tarefa_4_bronze_nvd_kev` | um registro CVE devolvido pelo NVD | preservar dados técnicos, CVSS e fraquezas |
| Silver | `tarefa_4_silver_vulnerabilidades` | uma linha por CVE | integrar, tipar, deduplicar e derivar indicadores |
| Gold | `tarefa_4_fato_vulnerabilidades` | uma vulnerabilidade explorada | medidas e chaves analíticas |
| Gold | `tarefa_4_dim_fornecedor` | um fornecedor/projeto | dimensão organizacional |
| Gold | `tarefa_4_dim_produto` | um produto por fornecedor | dimensão tecnológica |
| Gold | `tarefa_4_dim_tempo` | uma data de inclusão no KEV | dimensão temporal |
| Gold | `tarefa_4_dim_cwe` | uma categoria CWE | dimensão de fraqueza |
| Gold | `tarefa_4_ponte_cve_cwe` | um par CVE–CWE | resolve a relação muitos-para-muitos |

## Dicionário da fato

| Atributo | Tipo | Domínio/regra | Origem |
|---|---|---|---|
| `cve_id` | string | `CVE-AAAA-NNNN...`, obrigatório e único | CISA/NVD |
| `fornecedor_id` | string | SHA-256 do fornecedor | derivado |
| `produto_id` | string | SHA-256 de fornecedor + produto | derivado |
| `data_adicao` | date | data de entrada no KEV | CISA `dateAdded` |
| `data_publicacao` | timestamp | publicação do CVE | NVD `published` |
| `data_limite` | date | prazo definido pela CISA | CISA `dueDate` |
| `cvss_score` | double | 0–10; versão mais recente disponível | NVD `metrics` |
| `cvss_severidade` | string | LOW, MEDIUM, HIGH ou CRITICAL | NVD |
| `prioridade_cvss` | string | BAIXA, MEDIA, ALTA ou CRITICA | derivado do escore |
| `ransomware_confirmado` | boolean | verdadeiro apenas quando a CISA informa `Known` | CISA |
| `dias_publicacao_ate_kev` | inteiro | `data_adicao - data_publicacao` | derivado |
| `janela_correcao_dias` | inteiro | `data_limite - data_adicao` | derivado |
| `status_nvd` | string | situação editorial do CVE | NVD |
| `triagem_forense` | string | Yes/No | CISA |

## Regras de qualidade

- unicidade: uma linha por `cve_id` na Silver e na fato;
- validade: CVE conforme `^CVE-\d{4}-\d{4,}$`, fornecedor, produto e data de inclusão obrigatórios;
- integridade: integração externa auditada pela quantidade de chaves presentes em ambas as fontes;
- conformidade: CVSS entre 0 e 10, datas convertidas para tipos próprios e ransomware restrito a Known/Unknown;
- completude: nulos, vazios, cardinalidade, mínimo e máximo calculados por atributo;
- rastreabilidade: URL, versão do catálogo e horário de ingestão preservados na Bronze.

## Limitações semânticas

`Unknown` em uso de ransomware significa ausência de confirmação no catálogo, não prova de que nunca houve uso. Contagens por fornecedor não medem insegurança relativa: são influenciadas por base instalada, quantidade de produtos, longevidade e visibilidade. Diferenças negativas entre publicação NVD e inclusão CISA são mantidas como sinal de qualidade, pois alguns identificadores receberam publicação formal no NVD depois da entrada operacional no KEV.
