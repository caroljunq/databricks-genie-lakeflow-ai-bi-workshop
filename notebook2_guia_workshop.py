# Databricks notebook source

# MAGIC %md
# MAGIC # ⚡ Workshop Databricks — Time de Negócio | Setor de Energia
# MAGIC ### Guia Passo a Passo
# MAGIC
# MAGIC Bem-vindo(a)! Neste workshop você vai aprender a usar ferramentas da Databricks para **explorar, transformar e visualizar dados de energia** sem precisar sair do SQL.
# MAGIC
# MAGIC **Agenda do dia:**
# MAGIC
# MAGIC | # | Tópico | O que você vai fazer |
# MAGIC |---|---|---|
# MAGIC | 1 | Upload de arquivos | Fazer upload de CSVs e criar tabelas |
# MAGIC | 2 | Unity Catalog | Explorar catálogo, schema e volume |
# MAGIC | 3 | SQL Básico + AI Functions | Escrever queries e usar IA dentro do SQL |
# MAGIC | 4 | Lakeflow Designer | Construir um pipeline visual de dados |
# MAGIC | 5 | Genie Spaces | Fazer perguntas em linguagem natural |
# MAGIC | 6 | Databricks One | Visão geral da plataforma |
# MAGIC | 7 | AI/BI | Criar dashboards com linguagem natural |
# MAGIC
# MAGIC > **Contexto do dataset:** Trabalhamos com dados de uma distribuidora de energia elétrica com clientes Residenciais, Comerciais e Industriais em todo o Brasil.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📁 Módulo 1 — Upload de Arquivos e Criação de Tabelas
# MAGIC
# MAGIC ### O que é?
# MAGIC O Databricks permite fazer upload de arquivos (XLS, CSV, JSON) diretamente pela interface e convertê-los automaticamente em tabelas Delta no Unity Catalog — sem escrever uma linha de código.
# MAGIC
# MAGIC ### Passo a passo:
# MAGIC
# MAGIC 1. No menu lateral esquerdo, clique em **"+ New"** → **"Add or upload data"**
# MAGIC 2. Arraste o arquivo CSV ou clique para selecionar o arquivo do seu computador
# MAGIC 3. O Databricks detecta automaticamente o schema (tipos das colunas)
# MAGIC 4. Revise os tipos de dados e corrija se necessário
# MAGIC 5. Escolha o **Catálogo**, **Schema** e dê um **nome** para a tabela
# MAGIC 6. Clique em **"Create table"**
# MAGIC
# MAGIC ### Arquivos para fazer upload (gerados no Notebook 1):
# MAGIC - `clientes.csv` → Tabela: `clientes`
# MAGIC - `consumo.csv` → Tabela: `consumo`
# MAGIC - `instalacoes.csv` → Tabela: `instalacoes`
# MAGIC
# MAGIC > 💡 **Dica:** Para arquivos Excel (`.xlsx`), o processo é o mesmo. O Databricks converte cada aba em uma tabela separada.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🗂️ Módulo 2 — Unity Catalog: Catálogo, Schema e Volume
# MAGIC
# MAGIC ### O que é o Unity Catalog?
# MAGIC O Unity Catalog é o sistema de governança de dados da Databricks. Pense nele como um **gerenciador de arquivos inteligente** para dados, com três níveis hierárquicos:
# MAGIC
# MAGIC ```
# MAGIC Catálogo (Catalog)
# MAGIC └── Schema (Database)
# MAGIC     ├── Tabela (Table)   ← dados estruturados (linhas e colunas)
# MAGIC     ├── Volume           ← arquivos brutos (CSV, JSON, imagens, PDFs)
# MAGIC     └── View             ← consulta salva como "tabela virtual"
# MAGIC ```
# MAGIC
# MAGIC | Conceito | Analogia | Exemplo |
# MAGIC |---|---|---|
# MAGIC | **Catálogo** | Pasta principal | `main` |
# MAGIC | **Schema** | Subpasta por projeto | `workshop_energia` |
# MAGIC | **Tabela** | Planilha Excel | `clientes`, `consumo` |
# MAGIC | **Volume** | Área de arquivos brutos | `/Volumes/main/workshop_energia/arquivos` |
# MAGIC
# MAGIC ### Como explorar no Unity Catalog:
# MAGIC 1. No menu lateral, clique em **"Catalog"** (ícone de livro)
# MAGIC 2. Expanda seu catálogo → schema → veja as tabelas criadas
# MAGIC 3. Clique em uma tabela para ver **schema**, **sample data** e **lineage**
# MAGIC 4. Na aba **"Volumes"**, veja os arquivos CSVs que foram carregados
# MAGIC
# MAGIC > 💡 **Por que isso importa?** Todas as tabelas criadas aqui ficam disponíveis para todos os times da empresa, com controle de acesso centralizado.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 💻 Módulo 3 — SQL Básico, AI Functions, Notebook e SQL Editor
# MAGIC
# MAGIC ### 3.1 — SQL Editor
# MAGIC O SQL Editor é a ferramenta para escrever e executar queries SQL interativamente.
# MAGIC **Como acessar:** Menu lateral → **"SQL Editor"**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📝 Queries de Exemplo — Execute no SQL Editor
# MAGIC
# MAGIC #### Query 1 — Exploração básica: Ver os primeiros clientes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM main.workshop_energia.clientes
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 2 — Contagem por tipo de cliente

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   tipo_cliente,
# MAGIC   COUNT(*)          AS total_clientes,
# MAGIC   COUNT(CASE WHEN ativo = true THEN 1 END) AS clientes_ativos
# MAGIC FROM main.workshop_energia.clientes
# MAGIC GROUP BY tipo_cliente
# MAGIC ORDER BY total_clientes DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 3 — Consumo médio mensal por tipo de cliente

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   c.tipo_cliente,
# MAGIC   ROUND(AVG(con.kwh_consumido), 2) AS media_kwh,
# MAGIC   ROUND(AVG(con.valor_rs), 2)      AS media_valor_rs,
# MAGIC   ROUND(SUM(con.valor_rs), 2)      AS total_faturado_rs
# MAGIC FROM main.workshop_energia.consumo con
# MAGIC JOIN main.workshop_energia.clientes c
# MAGIC   ON con.cliente_id = c.cliente_id
# MAGIC GROUP BY c.tipo_cliente
# MAGIC ORDER BY total_faturado_rs DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 4 — Top 10 clientes por gasto total

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   c.nome,
# MAGIC   c.tipo_cliente,
# MAGIC   c.cidade,
# MAGIC   c.estado,
# MAGIC   ROUND(SUM(con.valor_rs), 2)    AS total_gasto_rs,
# MAGIC   ROUND(SUM(con.kwh_consumido), 2) AS total_kwh
# MAGIC FROM main.workshop_energia.consumo con
# MAGIC JOIN main.workshop_energia.clientes c
# MAGIC   ON con.cliente_id = c.cliente_id
# MAGIC GROUP BY c.nome, c.tipo_cliente, c.cidade, c.estado
# MAGIC ORDER BY total_gasto_rs DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 5 — Regra de negócio: Classificação de clientes por tier (Ouro / Prata / Bronze)
# MAGIC > **Regra:** Clientes com gasto médio mensal > R$5.000 = Ouro | > R$1.000 = Prata | demais = Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH gasto_mensal AS (
# MAGIC   SELECT
# MAGIC     c.cliente_id,
# MAGIC     c.nome,
# MAGIC     c.tipo_cliente,
# MAGIC     c.cidade,
# MAGIC     ROUND(AVG(con.valor_rs), 2) AS media_mensal_rs
# MAGIC   FROM main.workshop_energia.consumo con
# MAGIC   JOIN main.workshop_energia.clientes c
# MAGIC     ON con.cliente_id = c.cliente_id
# MAGIC   GROUP BY c.cliente_id, c.nome, c.tipo_cliente, c.cidade
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   CASE
# MAGIC     WHEN media_mensal_rs > 5000 THEN 'Ouro'
# MAGIC     WHEN media_mensal_rs > 1000 THEN 'Prata'
# MAGIC     ELSE 'Bronze'
# MAGIC   END AS tier_cliente
# MAGIC FROM gasto_mensal
# MAGIC ORDER BY media_mensal_rs DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 6 — Faturas vencidas: inadimplência por estado

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   c.estado,
# MAGIC   COUNT(*)                                              AS total_faturas,
# MAGIC   COUNT(CASE WHEN con.status_fatura = 'Vencida' THEN 1 END) AS faturas_vencidas,
# MAGIC   ROUND(
# MAGIC     COUNT(CASE WHEN con.status_fatura = 'Vencida' THEN 1 END) * 100.0 / COUNT(*), 1
# MAGIC   )                                                     AS pct_inadimplencia,
# MAGIC   ROUND(SUM(CASE WHEN con.status_fatura = 'Vencida' THEN con.valor_rs ELSE 0 END), 2) AS valor_em_atraso_rs
# MAGIC FROM main.workshop_energia.consumo con
# MAGIC JOIN main.workshop_energia.clientes c
# MAGIC   ON con.cliente_id = c.cliente_id
# MAGIC GROUP BY c.estado
# MAGIC ORDER BY valor_em_atraso_rs DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 7 — Equipamentos que precisam de manutenção (última manutenção há mais de 1 ano)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   i.instalacao_id,
# MAGIC   c.nome                           AS cliente,
# MAGIC   c.tipo_cliente,
# MAGIC   i.tipo_equipamento,
# MAGIC   i.fabricante,
# MAGIC   i.ultima_manutencao,
# MAGIC   DATEDIFF(CURRENT_DATE(), i.ultima_manutencao) AS dias_sem_manutencao,
# MAGIC   i.status
# MAGIC FROM main.workshop_energia.instalacoes i
# MAGIC JOIN main.workshop_energia.clientes c
# MAGIC   ON i.cliente_id = c.cliente_id
# MAGIC WHERE DATEDIFF(CURRENT_DATE(), i.ultima_manutencao) > 365
# MAGIC   AND i.status = 'Ativo'
# MAGIC ORDER BY dias_sem_manutencao DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 3.2 — AI Functions no SQL
# MAGIC As AI Functions permitem usar **IA generativa diretamente dentro de queries SQL**.
# MAGIC A função mais usada é `ai_gen()`, que gera texto com base em um prompt.
# MAGIC
# MAGIC #### Query 8 — `ai_gen`: Gerar análise automática de cliente em linguagem natural

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exemplo: gerar resumo automático do perfil de cada cliente
# MAGIC SELECT
# MAGIC   c.nome,
# MAGIC   c.tipo_cliente,
# MAGIC   c.cidade,
# MAGIC   ROUND(SUM(con.valor_rs), 2)      AS total_gasto_rs,
# MAGIC   ROUND(AVG(con.kwh_consumido), 2) AS media_kwh,
# MAGIC   ai_gen(
# MAGIC     CONCAT(
# MAGIC       'Você é um analista de uma distribuidora de energia. Escreva um resumo executivo em 2 frases sobre o cliente a seguir. ',
# MAGIC       'Nome: ', c.nome, '. ',
# MAGIC       'Tipo: ', c.tipo_cliente, '. ',
# MAGIC       'Cidade: ', c.cidade, '. ',
# MAGIC       'Gasto total: R$ ', ROUND(SUM(con.valor_rs), 2), '. ',
# MAGIC       'Consumo médio mensal: ', ROUND(AVG(con.kwh_consumido), 2), ' kWh. ',
# MAGIC       'Destaque os pontos mais relevantes para a equipe comercial.'
# MAGIC     )
# MAGIC   ) AS resumo_ia
# MAGIC FROM main.workshop_energia.consumo con
# MAGIC JOIN main.workshop_energia.clientes c
# MAGIC   ON con.cliente_id = c.cliente_id
# MAGIC GROUP BY c.nome, c.tipo_cliente, c.cidade
# MAGIC ORDER BY total_gasto_rs DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 9 — `ai_classify`: Classificar status de equipamento com IA

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ai_classify categoriza texto em classes predefinidas
# MAGIC SELECT
# MAGIC   tipo_equipamento,
# MAGIC   fabricante,
# MAGIC   status,
# MAGIC   DATEDIFF(CURRENT_DATE(), ultima_manutencao) AS dias_sem_manutencao,
# MAGIC   ai_classify(
# MAGIC     CONCAT('Equipamento: ', tipo_equipamento, '. Status: ', status,
# MAGIC            '. Dias sem manutenção: ', DATEDIFF(CURRENT_DATE(), ultima_manutencao)),
# MAGIC     ARRAY('Urgente', 'Atenção', 'Normal')
# MAGIC   ) AS prioridade_manutencao
# MAGIC FROM main.workshop_energia.instalacoes
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🔄 Módulo 4 — Lakeflow Designer: Pipeline Visual de Dados
# MAGIC
# MAGIC ### O que é o Lakeflow Designer?
# MAGIC O Lakeflow Designer é uma ferramenta **visual (drag-and-drop)** para construir pipelines de transformação de dados — sem escrever código. Ideal para times de negócio que precisam criar fluxos de dados com lógica de negócio.
# MAGIC
# MAGIC **Como acessar:** Menu lateral → **"Workflows"** → **"Lakeflow"** → **"Create pipeline"**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4.1 — Adicionar Data Sources (fontes de dados)
# MAGIC
# MAGIC **Passo a passo:**
# MAGIC 1. No canvas do Lakeflow, clique em **"+ Add source"**
# MAGIC 2. Selecione **"Unity Catalog table"**
# MAGIC 3. Busque e selecione a tabela `clientes`
# MAGIC 4. Repita para adicionar `consumo` e `instalacoes`
# MAGIC
# MAGIC > Você verá 3 nós de entrada no canvas, um para cada tabela.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4.2 — Join de Tabelas
# MAGIC
# MAGIC **Passo a passo:**
# MAGIC 1. Arraste uma conexão da tabela `consumo` para um novo nó **"Join"**
# MAGIC 2. Conecte também a tabela `clientes` ao mesmo nó Join
# MAGIC 3. Configure o Join:
# MAGIC    - **Join type:** Inner Join
# MAGIC    - **Left key:** `consumo.cliente_id`
# MAGIC    - **Right key:** `clientes.cliente_id`
# MAGIC 4. Selecione as colunas que deseja manter no resultado
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4.3 — Filtrar Dados
# MAGIC
# MAGIC **Passo a passo:**
# MAGIC 1. Após o Join, adicione um nó **"Filter"**
# MAGIC 2. Configure a condição de filtro. Exemplos:
# MAGIC    - Apenas clientes industriais: `tipo_cliente = 'Industrial'`
# MAGIC    - Apenas faturas do ano de 2025: `ano = 2025`
# MAGIC    - Apenas faturas vencidas: `status_fatura = 'Vencida'`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4.4 — Agregação (Soma, Média, Contagem)
# MAGIC
# MAGIC **Passo a passo:**
# MAGIC 1. Adicione um nó **"Aggregate"**
# MAGIC 2. Configure:
# MAGIC    - **Group by:** `tipo_cliente`, `estado` (ou `cidade`, `ano`, `mes`)
# MAGIC    - **Aggregations:**
# MAGIC      - `SUM(kwh_consumido)` → nome: `total_kwh`
# MAGIC      - `SUM(valor_rs)` → nome: `total_faturado_rs`
# MAGIC      - `COUNT(consumo_id)` → nome: `qtd_faturas`
# MAGIC      - `AVG(valor_rs)` → nome: `media_fatura_rs`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4.5 — Transformação com Regra de Negócio
# MAGIC
# MAGIC **Passo a passo:**
# MAGIC 1. Adicione um nó **"Derived column"** (coluna derivada)
# MAGIC 2. Crie uma nova coluna chamada `tier_cliente` com a fórmula:
# MAGIC    ```
# MAGIC    CASE
# MAGIC      WHEN media_fatura_rs > 5000 THEN 'Ouro'
# MAGIC      WHEN media_fatura_rs > 1000 THEN 'Prata'
# MAGIC      ELSE 'Bronze'
# MAGIC    END
# MAGIC    ```
# MAGIC 3. Outra coluna útil: `status_manutencao`:
# MAGIC    ```
# MAGIC    CASE
# MAGIC      WHEN dias_sem_manutencao > 365 THEN 'Atrasada'
# MAGIC      WHEN dias_sem_manutencao > 180 THEN 'Próxima'
# MAGIC      ELSE 'Em dia'
# MAGIC    END
# MAGIC    ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4.6 — Escrever Resultado em Tabela de Saída
# MAGIC
# MAGIC **Passo a passo:**
# MAGIC 1. Adicione um nó **"Write to table"** ao final do pipeline
# MAGIC 2. Configure:
# MAGIC    - **Catalog:** `main`
# MAGIC    - **Schema:** `workshop_energia`
# MAGIC    - **Table name:** `consumo_por_cliente_tier` (ou o nome que preferir)
# MAGIC    - **Write mode:** `Overwrite` (substitui a tabela a cada execução)
# MAGIC 3. Clique em **"Save"**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4.7 — Agendar Execução do Pipeline (Job)
# MAGIC
# MAGIC **Passo a passo:**
# MAGIC 1. No Lakeflow Designer, clique em **"Schedule"** (canto superior direito)
# MAGIC 2. Escolha a frequência:
# MAGIC    - **Manual:** executar sob demanda (botão "Run")
# MAGIC    - **Scheduled:** definir horário (ex: todo dia às 8h, toda segunda-feira)
# MAGIC    - **File arrival:** executar quando um novo arquivo chegar no Volume
# MAGIC 3. Configure alertas de email em caso de falha
# MAGIC 4. Clique em **"Save schedule"**
# MAGIC
# MAGIC > 💡 **Dica:** Você pode monitorar as execuções passadas em **"Workflow Runs"** → veja duração, status e logs de cada etapa.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🤖 Usando o Genie Code para Criar Nós no Lakeflow Designer
# MAGIC
# MAGIC O **Genie Code** permite descrever em linguagem natural o que você quer fazer, e ele gera automaticamente a configuração do nó no Lakeflow Designer.
# MAGIC
# MAGIC **Como usar:**
# MAGIC 1. No Lakeflow Designer, clique no ícone **"AI Assistant"** (varinha mágica ✨)
# MAGIC 2. Digite sua instrução em português ou inglês
# MAGIC 3. Revise o nó gerado e confirme
# MAGIC
# MAGIC **Exemplos de instruções para o Genie Code:**
# MAGIC
# MAGIC ```
# MAGIC "Faça um join entre a tabela consumo e a tabela clientes usando o campo cliente_id"
# MAGIC
# MAGIC "Filtre apenas os registros onde status_fatura é igual a Vencida e ano é 2025"
# MAGIC
# MAGIC "Some o kwh_consumido e o valor_rs agrupando por tipo_cliente e estado"
# MAGIC
# MAGIC "Crie uma coluna chamada tier_cliente: se valor_rs médio for maior que 5000 retorna Ouro,
# MAGIC  se maior que 1000 retorna Prata, caso contrário retorna Bronze"
# MAGIC
# MAGIC "Salve o resultado na tabela main.workshop_energia.consumo_consolidado em modo overwrite"
# MAGIC
# MAGIC "Adicione a tabela instalacoes como fonte de dados do pipeline"
# MAGIC
# MAGIC "Crie um nó de agregação que calcule a média de kwh_consumido por mês e por tipo de cliente"
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🧞 Módulo 5 — Genie Spaces: Dados em Linguagem Natural
# MAGIC
# MAGIC ### O que é o Genie Space?
# MAGIC O Genie Space é um **assistente de BI conversacional** da Databricks. Você faz perguntas em português (ou inglês) sobre seus dados e ele responde com tabelas, gráficos e explicações — sem precisar escrever SQL.
# MAGIC
# MAGIC **Como acessar:** Menu lateral → **"Genie"** → selecione ou crie um Space configurado com suas tabelas.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5.1 — Perguntas Comuns (modo padrão)
# MAGIC
# MAGIC Copie e cole estas perguntas diretamente no Genie Space:
# MAGIC
# MAGIC **Exploração geral:**
# MAGIC ```
# MAGIC Quantos clientes temos no total? Quebre por tipo de cliente.
# MAGIC ```
# MAGIC ```
# MAGIC Qual estado tem mais clientes industriais?
# MAGIC ```
# MAGIC ```
# MAGIC Qual foi o mês com maior consumo de energia em 2025?
# MAGIC ```
# MAGIC ```
# MAGIC Mostre o total faturado por estado em ordem decrescente.
# MAGIC ```
# MAGIC ```
# MAGIC Quais são os 10 clientes com maior consumo de energia?
# MAGIC ```
# MAGIC
# MAGIC **Inadimplência:**
# MAGIC ```
# MAGIC Quantas faturas estão vencidas? Qual é o valor total em atraso?
# MAGIC ```
# MAGIC ```
# MAGIC Qual tipo de cliente tem maior taxa de inadimplência?
# MAGIC ```
# MAGIC ```
# MAGIC Liste os clientes com faturas vencidas e o valor total de cada um.
# MAGIC ```
# MAGIC
# MAGIC **Equipamentos:**
# MAGIC ```
# MAGIC Quais equipamentos estão com status Em Manutenção?
# MAGIC ```
# MAGIC ```
# MAGIC Quantos equipamentos não receberam manutenção nos últimos 12 meses?
# MAGIC ```
# MAGIC ```
# MAGIC Qual fabricante tem mais equipamentos instalados?
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5.2 — Perguntas Hipotéticas com o Modo Agent (Deep Research)
# MAGIC
# MAGIC O modo **Agent** do Genie permite fazer **análises hipotéticas e exploratórias** mais profundas. Ative-o clicando em "Agent mode" dentro do Genie Space.
# MAGIC
# MAGIC **Exemplos de perguntas hipotéticas:**
# MAGIC
# MAGIC ```
# MAGIC Se aumentarmos a tarifa de Baixa Tensão em 10%, qual seria o impacto no faturamento total?
# MAGIC Quais clientes residenciais seriam mais afetados?
# MAGIC ```
# MAGIC ```
# MAGIC Se todos os clientes com faturas vencidas quitassem suas dívidas hoje,
# MAGIC qual seria o aumento percentual na receita do mês atual?
# MAGIC ```
# MAGIC ```
# MAGIC Baseado no histórico de consumo, quais clientes industriais têm maior risco
# MAGIC de aumentar o consumo acima da demanda contratada no próximo trimestre?
# MAGIC ```
# MAGIC ```
# MAGIC Se priorizarmos manutenção preventiva nos equipamentos Siemens e ABB com mais de
# MAGIC 365 dias sem revisão, quantos clientes seriam impactados e qual seria o custo estimado
# MAGIC considerando R$1.500 por visita técnica?
# MAGIC ```
# MAGIC ```
# MAGIC Analise a sazonalidade do consumo dos últimos 2 anos. Quando devo aumentar
# MAGIC a capacidade de atendimento e em quais regiões?
# MAGIC ```
# MAGIC ```
# MAGIC Se migrarmos todos os clientes Comerciais de Média Tensão para Alta Tensão,
# MAGIC qual seria a economia gerada na tarifa e qual o impacto na receita da distribuidora?
# MAGIC ```
# MAGIC ```
# MAGIC Crie um ranking de risco de churn dos clientes com base em: consumo declinante,
# MAGIC histórico de inadimplência e equipamentos com status Desativado.
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🌐 Módulo 6 — Databricks One: Visão Geral da Plataforma
# MAGIC
# MAGIC ### O que é?
# MAGIC O **Databricks One** é a experiência unificada da plataforma Databricks que integra em um único ambiente:
# MAGIC
# MAGIC | Ferramenta | Função |
# MAGIC |---|---|
# MAGIC | **Unity Catalog** | Governança e descoberta de dados |
# MAGIC | **SQL Editor** | Análise ad-hoc em SQL |
# MAGIC | **Notebooks** | Desenvolvimento em Python/SQL/R |
# MAGIC | **Lakeflow Designer** | Pipelines visuais de dados |
# MAGIC | **Genie Spaces** | BI conversacional com IA |
# MAGIC | **AI/BI Dashboards** | Dashboards interativos |
# MAGIC | **Workflows** | Orquestração e agendamento de jobs |
# MAGIC | **Marketplace** | Dados e modelos prontos para uso |
# MAGIC
# MAGIC ### Principais diferenciais:
# MAGIC - **Segurança centralizada:** controle de acesso por catálogo, schema e tabela
# MAGIC - **Colaboração:** notebooks e dashboards podem ser compartilhados com o time
# MAGIC - **Escalabilidade:** processa desde milhares até bilhões de registros
# MAGIC - **IA integrada:** AI Functions, Genie, e modelos de ML na mesma plataforma
# MAGIC
# MAGIC > 💡 **Para times de negócio:** Você não precisa conhecer toda a plataforma. Comece com SQL Editor e Genie — são suficientes para 80% das análises do dia a dia.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📊 Módulo 7 — AI/BI: Dashboards com Linguagem Natural
# MAGIC
# MAGIC ### O que é o AI/BI?
# MAGIC O **AI/BI** é o sistema de dashboards da Databricks que permite:
# MAGIC - Criar visualizações descrevendo o que você quer em linguagem natural
# MAGIC - Explorar dashboards fazendo perguntas diretamente para a IA
# MAGIC - Criar gráficos sem precisar configurar manualmente eixos, filtros e agrupamentos
# MAGIC
# MAGIC **Como acessar:** Menu lateral → **"Dashboards"** → **"New dashboard"**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7.1 — Criar Gráficos com Linguagem Natural
# MAGIC
# MAGIC **Passo a passo:**
# MAGIC 1. Clique em **"+ Add"** → **"Visualization"**
# MAGIC 2. No campo de prompt, descreva o gráfico que deseja criar. Exemplos:
# MAGIC
# MAGIC ```
# MAGIC Crie um gráfico de barras mostrando o total faturado por tipo de cliente
# MAGIC ```
# MAGIC ```
# MAGIC Mostre a evolução mensal do consumo de energia (kWh) em 2025 como um gráfico de linha
# MAGIC ```
# MAGIC ```
# MAGIC Crie um mapa do Brasil com o total de clientes por estado
# MAGIC ```
# MAGIC ```
# MAGIC Mostre um gráfico de pizza com a distribuição dos status de fatura
# MAGIC (Paga, Pendente, Vencida)
# MAGIC ```
# MAGIC ```
# MAGIC Crie um gráfico de barras empilhadas com o consumo total por mês e por tipo de cliente
# MAGIC ```
# MAGIC ```
# MAGIC Mostre um scatter plot com kwh_consumido no eixo X e valor_rs no eixo Y,
# MAGIC colorido por tipo_cliente
# MAGIC ```
# MAGIC ```
# MAGIC Crie um ranking dos 10 estados com maior valor de faturas vencidas
# MAGIC ```
# MAGIC ```
# MAGIC Mostre a distribuição dos equipamentos por fabricante em um gráfico de barras horizontais
# MAGIC ```
# MAGIC
# MAGIC **Dica:** Adicione um **filtro de data** para tornar o dashboard interativo:
# MAGIC - Clique em **"+ Add filter"** → selecione o campo `data_leitura` ou `ano`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7.2 — Explorar os Gráficos com Perguntas para o AI/BI
# MAGIC
# MAGIC Após criar o dashboard, clique no ícone de **chat com IA** (canto inferior direito) e explore:
# MAGIC
# MAGIC **Perguntas de análise:**
# MAGIC ```
# MAGIC Por que o consumo de outubro a dezembro é sempre maior?
# MAGIC ```
# MAGIC ```
# MAGIC Quais estados apresentam crescimento de consumo acima da média nacional?
# MAGIC ```
# MAGIC ```
# MAGIC Existe correlação entre o tipo de equipamento instalado e o consumo mensal?
# MAGIC ```
# MAGIC ```
# MAGIC O gráfico mostra sazonalidade? Em quais meses o consumo é maior?
# MAGIC ```
# MAGIC
# MAGIC **Perguntas de negócio:**
# MAGIC ```
# MAGIC Qual tipo de cliente é mais rentável para a empresa?
# MAGIC ```
# MAGIC ```
# MAGIC Quais estados merecem maior atenção comercial pelo volume de clientes
# MAGIC com alto potencial de consumo?
# MAGIC ```
# MAGIC ```
# MAGIC Baseado no gráfico de inadimplência, em quais regiões devemos intensificar
# MAGIC a cobrança?
# MAGIC ```
# MAGIC ```
# MAGIC Estou vendo um pico de consumo em janeiro. O que pode explicar isso?
# MAGIC ```
# MAGIC
# MAGIC **Perguntas de drill-down:**
# MAGIC ```
# MAGIC Clique no estado SP no gráfico — quais cidades contribuem mais para o consumo?
# MAGIC ```
# MAGIC ```
# MAGIC Filtre apenas clientes Industriais. Como muda o ranking de estados?
# MAGIC ```
# MAGIC ```
# MAGIC Compare o faturamento de 2024 vs 2025. Houve crescimento?
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7.3 — Compartilhar o Dashboard
# MAGIC
# MAGIC 1. Clique em **"Share"** (canto superior direito)
# MAGIC 2. Adicione membros do time ou gere um link público
# MAGIC 3. Defina permissões: **Can view** (apenas visualizar) ou **Can edit** (editar)
# MAGIC 4. O dashboard atualiza automaticamente conforme novas tabelas são gravadas pelo Lakeflow

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🎯 Resumo do Workshop
# MAGIC
# MAGIC Parabéns! Você completou o workshop. Aqui está o que você aprendeu:
# MAGIC
# MAGIC | Módulo | Habilidade adquirida |
# MAGIC |---|---|
# MAGIC | 1 | Fazer upload de arquivos e criar tabelas no Databricks |
# MAGIC | 2 | Navegar no Unity Catalog e entender a hierarquia de dados |
# MAGIC | 3 | Escrever queries SQL com lógica de negócio e usar AI Functions |
# MAGIC | 4 | Construir pipelines de dados visuais no Lakeflow Designer |
# MAGIC | 5 | Fazer perguntas sobre dados em linguagem natural no Genie |
# MAGIC | 6 | Conhecer a plataforma Databricks One como um todo |
# MAGIC | 7 | Criar dashboards interativos com IA no AI/BI |
# MAGIC
# MAGIC ### Próximos passos sugeridos:
# MAGIC 1. **Traga seus próprios dados** — faça upload de um XLS do seu time e repita o exercício
# MAGIC 2. **Configure um Genie Space** com as tabelas do seu projeto real
# MAGIC 3. **Automatize** o pipeline do Lakeflow para rodar diariamente
# MAGIC 4. **Compartilhe** o dashboard com stakeholders da empresa
# MAGIC
# MAGIC > 📬 **Dúvidas?** Fale com a equipe Databricks ou acesse a documentação em [docs.databricks.com](https://docs.databricks.com)
