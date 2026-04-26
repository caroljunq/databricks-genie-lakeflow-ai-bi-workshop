# databricks-genie-lakeflow-ai-bi-workshop

Repositório de apoio ao workshop para times de negócio do setor de energia elétrica. O material cobre desde a geração de dados sintéticos até a construção de pipelines visuais, dashboards e consultas em linguagem natural.

---

## Estrutura do Repositório

```
.
├── notebook1_gerar_dataset.ipynb   # Gera e publica o dataset sintético
├── notebook2_guia_workshop.ipynb   # Guia passo a passo do workshop
└── dados/
    ├── clientes.csv                # Cadastro de clientes
    ├── consumo.csv                 # Histórico mensal de consumo
    └── instalacoes.csv             # Equipamentos elétricos instalados
```

---

## Notebook 1 — `notebook1_gerar_dataset.ipynb`

**Finalidade:** Gera um dataset sintético e realístico de uma distribuidora de energia elétrica com 200 clientes e publica os dados no Databricks Unity Catalog.

### O que o notebook faz

| Passo | Descrição |
|---|---|
| 1 | Configura parâmetros via widgets (catálogo, schema, prefixo e caminho do volume) |
| 2 | Importa bibliotecas Python (`pandas`, `numpy`, `pyspark`) |
| 3 | Cria o schema e o volume no Unity Catalog |
| 4 | Gera os dados das três tabelas com regras de negócio realísticas |
| 5 | Salva as tabelas no formato Delta no Unity Catalog |
| 6 | Exporta os arquivos CSV para o Volume configurado |
| 7 | Exibe um resumo final com nome das tabelas, caminhos e quantidade de linhas |

### Parâmetros obrigatórios (widgets)

| Widget | Exemplo | Descrição |
|---|---|---|
| `nome_catalogo` | `main` | Catálogo do Unity Catalog |
| `nome_schema` | `workshop_energia` | Schema onde as tabelas serão criadas |
| `seu_prefixo` | `carol_` | Prefixo único para evitar conflito entre participantes |
| `caminho_volume` | `/Volumes/main/workshop_energia/arquivos/` | Caminho do Volume para salvar os CSVs |

### Características dos dados gerados

- **200 clientes** divididos entre Residencial (55%), Comercial (30%) e Industrial (15%)
- Distribuídos em **20 cidades brasileiras**
- **Histórico de consumo** com sazonalidade (verão = maior consumo) e tarifas diferenciadas por tensão
- **Equipamentos** com variação de quantidade por porte do cliente (1 para residencial, até 5 para industrial)

---

## Notebook 2 — `notebook2_guia_workshop.ipynb`

**Finalidade:** Guia prático passo a passo para times de negócio explorarem as ferramentas da plataforma Databricks usando o dataset de energia gerado no Notebook 1.

### Módulos do workshop

| # | Módulo | O que você faz |
|---|---|---|
| 1 | **Upload de Arquivos** | Fazer upload dos CSVs e criar tabelas no Unity Catalog via interface gráfica |
| 2 | **Unity Catalog** | Navegar na hierarquia catálogo → schema → tabela/volume e explorar lineage |
| 3 | **SQL + AI Functions** | Escrever queries de negócio e usar `ai_gen` / `ai_classify` dentro do SQL |
| 4 | **Lakeflow Designer** | Construir pipeline visual de dados (join, filtro, agregação, transformação) |
| 5 | **Genie Spaces** | Fazer perguntas sobre os dados em linguagem natural (modo padrão e modo Agent) |
| 6 | **Databricks One** | Visão geral da plataforma integrada |
| 7 | **AI/BI Dashboards** | Criar dashboards e gráficos usando linguagem natural |

### Queries SQL de exemplo incluídas

| Query | Descrição |
|---|---|
| 1 | Exploração básica da tabela de clientes |
| 2 | Contagem de clientes ativos por tipo |
| 3 | Consumo médio mensal e faturamento total por tipo de cliente |
| 4 | Top 10 clientes por gasto total |
| 5 | Classificação de clientes em tiers Ouro / Prata / Bronze |
| 6 | Inadimplência por estado (% de faturas vencidas e valor em atraso) |
| 7 | Equipamentos ativos sem manutenção há mais de 1 ano |
| 8 | `ai_gen` — resumo executivo de cada cliente gerado por IA |
| 9 | `ai_classify` — prioridade de manutenção classificada por IA |
| 10–11 | Criação de tabelas a partir de arquivos CSV/XLS no Volume |

---

## Pasta `dados/`

Contém os arquivos CSV pré-gerados pelo Notebook 1, prontos para upload manual no Databricks via interface gráfica (Módulo 1 do workshop).

| Arquivo | Tabela correspondente | Descrição |
|---|---|---|
| `clientes.csv` | `clientes` | Cadastro de clientes |
| `consumo.csv` | `consumo` | Histórico mensal de consumo e faturamento |
| `instalacoes.csv` | `instalacoes` | Equipamentos elétricos instalados por cliente |

---

## Schema das Tabelas

### Tabela `clientes`

Cadastro de 200 clientes residenciais, comerciais e industriais distribuídos em cidades brasileiras.

| Coluna | Tipo | Descrição |
|---|---|---|
| `cliente_id` | INTEGER | Identificador único do cliente |
| `nome` | STRING | Nome do cliente ou razão social da empresa |
| `tipo_cliente` | STRING | Tipo de cliente: `Residencial`, `Comercial` ou `Industrial` |
| `cidade` | STRING | Cidade do cliente |
| `estado` | STRING | Sigla do estado (UF) |
| `tensao` | STRING | Nível de tensão da ligação: `Baixa Tensão`, `Média Tensão` ou `Alta Tensão` |
| `data_cadastro` | DATE | Data de cadastro do cliente (entre 2018 e 2024) |
| `ativo` | BOOLEAN | Status de atividade do cliente (`true` = ativo, `false` = inativo) |

**Valores possíveis — `tipo_cliente` × `tensao`:**

| tipo_cliente | tensao |
|---|---|
| Residencial | Baixa Tensão |
| Comercial | Média Tensão |
| Industrial | Alta Tensão |

---

### Tabela `consumo`

Histórico mensal de leituras de consumo de energia por cliente, com valor faturado e status de pagamento.

| Coluna | Tipo | Descrição |
|---|---|---|
| `consumo_id` | INTEGER | Identificador único do registro de consumo |
| `cliente_id` | INTEGER | Referência ao cliente (chave estrangeira → `clientes.cliente_id`) |
| `data_leitura` | DATE | Data em que a leitura foi realizada |
| `mes` | INTEGER | Mês da leitura (1–12) |
| `ano` | INTEGER | Ano da leitura |
| `kwh_consumido` | FLOAT | Quantidade de energia consumida em kWh |
| `demanda_kw` | FLOAT | Demanda de energia em kW |
| `valor_rs` | FLOAT | Valor da fatura em reais (R$) |
| `status_fatura` | STRING | Status do pagamento: `Paga`, `Pendente` ou `Vencida` |

**Faixas de consumo por tipo de cliente:**

| tipo_cliente | kwh_consumido (base) | Tarifa (R$/kWh) |
|---|---|---|
| Residencial | 150 – 500 kWh | R$ 0,78 |
| Comercial | 800 – 6.000 kWh | R$ 0,52 |
| Industrial | 10.000 – 80.000 kWh | R$ 0,34 |

**Sazonalidade:** consumo 30% maior em janeiro e 15% menor em junho/julho.

**Distribuição de status:** Paga 75% · Pendente 15% · Vencida 10%

---

### Tabela `instalacoes`

Equipamentos elétricos instalados por cliente: tipo, fabricante, capacidade, datas e status de manutenção.

| Coluna | Tipo | Descrição |
|---|---|---|
| `instalacao_id` | INTEGER | Identificador único da instalação |
| `cliente_id` | INTEGER | Referência ao cliente (chave estrangeira → `clientes.cliente_id`) |
| `tipo_equipamento` | STRING | Tipo de equipamento instalado |
| `fabricante` | STRING | Fabricante do equipamento |
| `capacidade_kw` | FLOAT | Capacidade nominal do equipamento em kW |
| `data_instalacao` | DATE | Data em que o equipamento foi instalado |
| `ultima_manutencao` | DATE | Data da última manutenção realizada |
| `status` | STRING | Estado atual do equipamento |
| `vida_util_anos` | INTEGER | Vida útil estimada em anos (5–25) |

**Tipos de equipamento disponíveis:**
`Medidor Digital`, `Transformador`, `Quadro de Distribuição`, `No-break Industrial`, `Gerador Diesel`, `Painéis Solares`, `Banco de Capacitores`, `Disjuntor Geral`, `Subestação`, `UPS`

**Fabricantes disponíveis:**
`Schneider Electric`, `ABB`, `Siemens`, `WEG`, `Eaton`, `GE`, `Philips`, `Delta`, `Emerson`, `Hitachi`

**Valores possíveis — `status`:**

| status | Probabilidade |
|---|---|
| Ativo | 70% |
| Em Manutenção | 15% |
| Desativado | 8% |
| Substituído | 7% |

**Quantidade de instalações por tipo de cliente:**

| tipo_cliente | Qtd. instalações |
|---|---|
| Residencial | 1 |
| Comercial | 1–3 |
| Industrial | 2–5 |

---

## Relações entre as Tabelas

```
clientes (cliente_id) ──< consumo    (cliente_id)
clientes (cliente_id) ──< instalacoes (cliente_id)
```

As três tabelas se relacionam pelo campo `cliente_id`, permitindo joins para análises cruzadas entre perfil do cliente, histórico de consumo e equipamentos instalados.
