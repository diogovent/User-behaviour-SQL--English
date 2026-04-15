============================================================
Rewards Programme — Twitch Platform / Courses
============================================================
Objective: To build an analytical table containing the
complete behavioural profile of each user.

**Important**
    This is merely a document for reference; to run the programme, you must use the ‘etl.projeto.sql’ file located in this repository
    
    
 Metrics generated:
    • Number of transactions         (Lifetime, D7, D14, D28, D56)
    • Days since last transaction    (recency)
    • Age in the database                    (days since registration)
    • Most used product               (Lifetime, D7, D14, D28, D56)
    • Category of most used product  (Lifetime, D7, D14, D28, D56)
    • Current points balance
    • Positive accumulated points      (Lifetime, D7, D14, D28, D56)
    • Negative accumulated points      (Lifetime, D7, D14, D28, D56)
    • Most active day of the week         (D28)
    • Most active time of day        (D28)
    • D28 engagement vs. Lifetime
    • Connected platforms           (Twitch, YouTube, Email, etc.)

Compatibility: SQLite
    
⚠️ Important note regarding coverage:

    The database has 4,962 registered customers, of whom 1,469 (~30%) have never carried out any transactions. 
    
    As the output is taken from `db_transaction_summary` (which only aggregates customers with at least one transaction), 
    these inactive customers are EXCLUDED from the final result. To include them, the starting point should 
    be the `clientes` table with LEFT JOINs.
    
---

1. tb_transações
    Cleaning and enrichment of the raw transaction table.
    
        • substr(DtCriacao, 1, 19) → removes milliseconds and time zones, 
    ensuring the format “YYYY-MM-DD HH:MM:SS” is compatible with the 
    SQLite datetime() and strftime() functions.
    
        • Diff_Date: number of calendar days since the transaction (float). Values close to 0 = recent transactions.
    
        • Dt_Hora: time extracted as an integer for classifying the time of day in the CTE tb_cliente_periodo.

--------------------------------------------------------------
WITH tb_transações AS (
    SELECT
        IdTransacao,
        IdCliente,
        QtdePontos,
        datetime(substr(DtCriacao, 1, 19))                          AS Dt_Criação,
        julianday('now') - julianday(substr(DtCriacao, 1, 10))      AS Diff_Date,
        CAST(strftime('%H', substr(DtCriacao, 1, 19)) AS INTEGER)   AS Dt_Hora

    FROM transacoes
),

---
    
2. tb_cliente
    
    Extracts registration data and platform flags for each customer.
    
        • Age_Base: number of calendar days since registration. Allows you to segment new customers from established ones.
    
        • Platform flags (1 = connected, 0 = not connected): flTwitch (primary, ~69% of customers), 
    flEmail (~4%), flYouTube (~3%), flBlueSky and flInstagram (not yet in use).

--------------------------------------------------------------  
tb_cliente as (
    SELECT idCliente,
           datetime(substr(DtCriacao,1,19)) as Dt_Criação,
           julianday("now") - julianday(substr(DtCriacao,1,10)) as Idade_Base
    
    From clientes
),

---
    
3. tb_sumário_transações
    
    Central aggregation by customer — all volume, recency and points metrics segmented by time window.
    
        • Windows: Lifetime (full history), D56, D28, D14, D7.
    
        • Days_Since_Last_Interaction: MIN(Diff_Date) = most recent transaction. The lower the value, the more recent the customer.
    
        • Points_Balance: sum of all transactions (positive = credit, negative = redemption). 
    Validated against customers.points_count — the values match exactly.
    
        • Negative points are retained as negative values, facilitating the credit/debit distinction in the analysis. 
    In the current database, redemptions account for < 1% of transactions.
    
--------------------------------------------------------------
tb_sumário_transações AS (

    SELECT
        IdCliente,

          **Transaction volume by time period**
        COUNT(IdTransacao)                                                              AS Qt_Transações_Vida,
        COUNT(CASE WHEN Diff_Date <= 56 THEN IdTransacao END)                           AS Qt_Transações_D56,
        COUNT(CASE WHEN Diff_Date <= 28 THEN IdTransacao END)                           AS Qt_Transações_D28,
        COUNT(CASE WHEN Diff_Date <= 14 THEN IdTransacao END)                           AS Qt_Transações_D14,
        COUNT(CASE WHEN Diff_Date <=  7 THEN IdTransacao END)                           AS Qt_Transações_D7,

          **Recency: the number of days since the last interaction**
        MIN(Diff_Date)                                                                   AS Dias_Ultima_Interação,

          **Current balance = accumulated credits − redemptions**
        SUM(QtdePontos)                                                                  AS Saldo_Pontos,

         **Credits (points earned) per window**
        SUM(CASE WHEN QtdePontos >  0                     THEN QtdePontos ELSE 0 END)   AS Pontos_Positivos_Vida,
        SUM(CASE WHEN QtdePontos >  0 AND Diff_Date <= 56 THEN QtdePontos ELSE 0 END)   AS Pontos_Positivos_D56,
        SUM(CASE WHEN QtdePontos >  0 AND Diff_Date <= 28 THEN QtdePontos ELSE 0 END)   AS Pontos_Positivos_D28,
        SUM(CASE WHEN QtdePontos >  0 AND Diff_Date <= 14 THEN QtdePontos ELSE 0 END)   AS Pontos_Positivos_D14,
        SUM(CASE WHEN QtdePontos >  0 AND Diff_Date <=  7 THEN QtdePontos ELSE 0 END)   AS Pontos_Positivos_D7,

          **Debits (point redemptions) per window — negative values**
        SUM(CASE WHEN QtdePontos <  0                     THEN QtdePontos ELSE 0 END)   AS Pontos_Negativos_Vida,
        SUM(CASE WHEN QtdePontos <  0 AND Diff_Date <= 56 THEN QtdePontos ELSE 0 END)   AS Pontos_Negativos_D56,
        SUM(CASE WHEN QtdePontos <  0 AND Diff_Date <= 28 THEN QtdePontos ELSE 0 END)   AS Pontos_Negativos_D28,
        SUM(CASE WHEN QtdePontos <  0 AND Diff_Date <= 14 THEN QtdePontos ELSE 0 END)   AS Pontos_Negativos_D14,
        SUM(CASE WHEN QtdePontos <  0 AND Diff_Date <=  7 THEN QtdePontos ELSE 0 END)   AS Pontos_Negativos_D7

    FROM tb_transações
    GROUP BY IdCliente
),

---

4. tb_transação_produto
    
    Enrich each transaction with the product name and category.

    Join structure:
        transactions ──(1:N)── transaction_product ──(N:1)── products

    Join coverage: 99.86% of rows have an identified product.

    The ~0.14% without a match correspond to an empty ProductID (“”).

    The products are divided into two groups:

        1. Engagement actions: “ChatMessage”, “Attendance List”, “Attendance Streak” — account for > 97% of the volume.
    
        2. RPG items (swords, armour, etc.) — redemption transactions, where the customer spends accumulated points.

--------------------------------------------------------------
tb_transação_produto AS (

    SELECT
        t1.*,
        t3.DescNomeProduto,
        t3.DescCategoriaProduto

    FROM tb_transações AS t1

    LEFT JOIN transacao_produto AS t2
        ON t1.IdTransacao = t2.IdTransacao

    LEFT JOIN produtos AS t3
        ON t2.IdProduto = t3.IdProduto
),
    
---
    
5. tb_cliente_produto

    Counts how many times each customer has interacted with each product, by time period.
    
    Includes ProductCategory to enrich the final profile beyond just the name of the most frequently used product.
    
-- ------------------------------------------------------------
tb_cliente_produto AS (

    SELECT
        IdCliente,
        DescNomeProduto,
        DescCategoriaProduto,
        COUNT(*)                                                        AS Qt_Vida,
        COUNT(CASE WHEN Diff_Date <= 56 THEN IdTransacao END)          AS Qt_D56,
        COUNT(CASE WHEN Diff_Date <= 28 THEN IdTransacao END)          AS Qt_D28,
        COUNT(CASE WHEN Diff_Date <= 14 THEN IdTransacao END)          AS Qt_D14,
        COUNT(CASE WHEN Diff_Date <=  7 THEN IdTransacao END)          AS Qt_D7

    FROM tb_transação_produto
    GROUP BY IdCliente, DescNomeProduto, DescCategoriaProduto
),

---
    
6. tb_cliente_produto_rn
    
    Rank products by frequency of use in each window.
    
    RN = 1 identifies the most frequently used product.

    ⚠️  In the event of a tie, ROW_NUMBER() breaks the tie by sorting on ProductName ASC to ensure determinism.
    
--------------------------------------------------------------
tb_cliente_produto_rn AS (

    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY IdCliente ORDER BY Qt_Vida DESC, DescNomeProduto) AS RN_Vida,
        ROW_NUMBER() OVER (PARTITION BY IdCliente ORDER BY Qt_D56  DESC, DescNomeProduto) AS RN_D56,
        ROW_NUMBER() OVER (PARTITION BY IdCliente ORDER BY Qt_D28  DESC, DescNomeProduto) AS RN_D28,
        ROW_NUMBER() OVER (PARTITION BY IdCliente ORDER BY Qt_D14  DESC, DescNomeProduto) AS RN_D14,
        ROW_NUMBER() OVER (PARTITION BY IdCliente ORDER BY Qt_D7   DESC, DescNomeProduto) AS RN_D7

    FROM tb_cliente_produto
),

---
    
7. tb_cliente_dia
    
    Counts transactions by day of the week over the last 28 days.
    
    strftime(“%w”, ...) returns TEXT:
    
        “0”=Sunday | “1”=Monday | “2”=Tuesday | “3”=Wednesday | “4”=Thursday | “5”=Friday | “6”=Saturday
    
--------------------------------------------------------------
tb_cliente_dia AS (

    SELECT
        IdCliente,
        strftime('%w', Dt_Criação)  AS Dia_Semana,
        COUNT(*)                    AS Qt_Transações

    FROM tb_transações
    WHERE Diff_Date <= 28
    GROUP BY IdCliente, Dia_Semana
),

---
    
8. tb_cliente_dia_rn
    
    Rank the days of the week by transaction volume.
    
    RN = 1 → the busiest day in the last 28 days.
    
    Tie-break by Day_of_Week ASC for determinism.
--------------------------------------------------------------
tb_cliente_dia_rn AS (

    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY IdCliente ORDER BY Qt_Transações DESC, Dia_Semana) AS RN_Dia

    FROM tb_cliente_dia
),

---

9. tb_cliente_periodo
    
    Sorts transactions from the last 28 days by time of day and calculates the total per customer and time period.
    
    Defined time slots:
    
        Morning:     07:00 – 12:00
        Afternoon:     13:00 – 18:00
        Evening:     19:00 – 23:00
        Early morning: 00:00 – 06:00
    
--------------------------------------------------------------
tb_cliente_periodo AS (

    SELECT
        IdCliente,
        CASE
            WHEN Dt_Hora BETWEEN  7 AND 12 THEN 'Manhã'
            WHEN Dt_Hora BETWEEN 13 AND 18 THEN 'Tarde'
            WHEN Dt_Hora BETWEEN 19 AND 23 THEN 'Noite'
            ELSE 'Madrugada'
        END                         AS Periodo,
        COUNT(*)                    AS Qt_Transações

    FROM tb_transações
    WHERE Diff_Date <= 28
    GROUP BY IdCliente, Periodo
),

---
    
10. tb_cliente_periodo_rn
    
    Ranks periods by transaction volume.
    
    RN = 1 → the most active period in the last 28 days.
    
--------------------------------------------------------------
tb_cliente_periodo_rn AS (

    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY IdCliente ORDER BY Qt_Transações DESC, Periodo) AS RN_Periodo

    FROM tb_cliente_periodo
),

---
    
11. tb_join
    
    Consolidates all CTEs into a single row per customer.
    
    COALESCE ensures that customers with no activity in the last 28 days do not generate NULLs in the output:
    
        • Day_of_Week_D28: “N/A” (text, consistent with strftime)
        • Period_D28:    'No Information'
    
--------------------------------------------------------------
tb_join AS (

    SELECT
        t1.*,
        t2.Idade_Base,

            **Most popular products and categories by time period**
    
        t3.DescNomeProduto      AS Produto_Vida,
        t3.DescCategoriaProduto AS Categoria_Produto_Vida,

        t4.DescNomeProduto      AS Produto_D56,
        t4.DescCategoriaProduto AS Categoria_Produto_D56,

        t5.DescNomeProduto      AS Produto_D28,
        t5.DescCategoriaProduto AS Categoria_Produto_D28,

        t6.DescNomeProduto      AS Produto_D14,
        t6.DescCategoriaProduto AS Categoria_Produto_D14,

        t7.DescNomeProduto      AS Produto_D7,
        t7.DescCategoriaProduto AS Categoria_Produto_D7,

            **The busiest day of the week in D28**
    
        COALESCE(t8.Dia_Semana, 'N/A')          AS Dia_Semana_Mais_Ativo_D28,

            **The busiest time of day at D28**
    
        COALESCE(t9.Periodo, 'Sem Informação')  AS Periodo_Mais_Ativo_D28

    FROM tb_sumário_transações AS t1

    LEFT JOIN tb_cliente AS t2
        ON t1.IdCliente = t2.idCliente

        **Most popular product — Life**
    
    LEFT JOIN tb_cliente_produto_rn AS t3
        ON t1.IdCliente = t3.IdCliente AND t3.RN_Vida = 1

        **Most popular product — D56**
    
    LEFT JOIN tb_cliente_produto_rn AS t4
        ON t1.IdCliente = t4.IdCliente AND t4.RN_D56  = 1

        **Most popular product — D28**
    
    LEFT JOIN tb_cliente_produto_rn AS t5
        ON t1.IdCliente = t5.IdCliente AND t5.RN_D28  = 1

        **Most popular product — D14**
    
    LEFT JOIN tb_cliente_produto_rn AS t6
        ON t1.IdCliente = t6.IdCliente AND t6.RN_D14  = 1

        **Most popular product — D7**
    
    LEFT JOIN tb_cliente_produto_rn AS t7
        ON t1.IdCliente = t7.IdCliente AND t7.RN_D7   = 1

        **The busiest day at D28**
    
    LEFT JOIN tb_cliente_dia_rn AS t8
        ON t1.IdCliente = t8.IdCliente AND t8.RN_Dia  = 1

        **Most active period on D28**
    
    LEFT JOIN tb_cliente_periodo_rn AS t9
        ON t1.IdCliente = t9.IdCliente AND t9.RN_Periodo = 1
)

---
    
FINAL OUTPUT
    
    One row per customer containing all metrics.
    
    D28_Engagement_Lifetime:
    
        Proportion of D28 transactions relative to the total history.
        Range: 0.0 (inactive in D28) → 1.0 (all activity in D28).
        NULLIF prevents division by zero for customers with no history.
--------------------------------------------------------------
SELECT
    *,
    1.0 * Qt_Transações_D28 / NULLIF(Qt_Transações_Vida, 0)  AS Engajamento_D28_Vida

FROM tb_join;
