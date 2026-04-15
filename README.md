# 👤 User-behaviour---SQL--English

> Building a comprehensive behavioural profile of users of a loyalty programme based on the **Twitch** platform, using pure SQL (SQLite).

---

## 📋 Project Background

This is a loyalty programme in which users earn points through engagement activities on Twitch (chat messages, watching streams, streaks) and can redeem these points for items from an RPG universe (swords, armour, staffs, etc.).

The query builds a **one-row-per-user analytical table** ready for segmentation, clustering, churn modelling or CRM dashboards.

The data was taken from this website: https://www.kaggle.com/datasets/teocalvo/teomewhy-loyalty-system

To run the project on your machine, you need to download the file ‘etl_projeto.sql’; if you’d like to see my notes, you can open (via GitHub) the file ‘user_behavioral_profile.sql’.

---

## 🗃️ Database

| Table | Rows | Description |
|---|---|---|
| `clientes` | 4,962 | User records, points balance and platform flags |
| `transacoes` | 293,615 | Points transactions (credits and debits) |
| `transacao_produto` | 293,897 | Link between transactions and products |
| `produtos` | 118 | Catalogue of engagement actions and RPG items |
| `clientes_d28` | 160 | Auxiliary table of recent activity |
| `relatorio_diario` | 618 | Cumulative report of transactions per day |

### Transaction sources (`DescSistemaOrigem`)
| Platform | Transactions | % |
|---|---|---|
| Twitch | 293,171 | 99.8% |
| Courses | 444 | 0.2% |

### Most frequent products
| Product | Category | Transactions |
|---|---|---|
| ChatMessage | chat | 242,010 |
| Lista de presença | present | 40,350 |
| Presença Streak | present | 2,989 |
| Resgatar Ponei | ponei | 1,900 |
| Churn_5pp / Churn_2pp / Churn_10pp | churn_model | 3,706 |

> RPG items (espadas, armaduras, etc.) only appear in **redeem** transactions, where the customer **spends** accumulated points.

---

## 📦 Generated Metrics

| Category | Metric | Timeframes |
|---|---|---|
| 🔁 Transactions | Number of transactions | Lifetime, D7, D14, D28, D56 |
| ⏱️ Recency | Days since last transaction | — |
| 🗓️ Tenure | Days since registration | — |
| 🛒 Product | Most used product | Lifetime, D7, D14, D28, D56 |
| 🏷️ Category | Category of most used product | Lifetime, D7, D14, D28, D56 |
| 💰 Balance | Current points balance | — |
| ➕ Credits | Points received | Life, D7, D14, D28, D56 |
| ➖ Debits | Points spent (redemptions) | Life, D7, D14, D28, D56 |
| 📅 Day of the week | Day with most transactions | D28 |
| 🌓 Time of day | Morning / Afternoon / Evening / Late night | D28 |
| 📊 Engagement | Proportion of D28 / Lifetime transactions | — |
| 🔗 Platforms | Twitch, YouTube, Email, BlueSky, Instagram connected | — |

> **Time frames:** `Lifetime` = full history · `D7/D14/D28/D56` = last 7, 14, 28 or 56 days.

---

## 🗂️ Query Architecture

My project is organised into **11 chained CTEs**:

    transacoes             clientes         transacao_produto   produtos
         │                    │                     │               │
         ▼                    ▼                     └──────┬────────┘
    tb_transações         tb_cliente                       ▼
         │                    │               tb_transação_produto
         │                    │                       │
         ▼                    │               tb_cliente_produto
    tb_sumário_transações     │                       │
         │                    │               tb_cliente_produto_rn
         │                    │                       │
    tb_cliente_dia            │               ┌───────┘
         │                    │               │
    tb_cliente_dia_rn         │               │
         │                    │               │
    tb_cliente_periodo        │               │
         │                    │               │
    tb_cliente_periodo_rn     │               │
         │                    │               │
         └────────────────────┴───────────────┘
                              │
                          tb_join
                              │
                       SELECT final
                   (+ Engajamento_D28_Vida)

### Description of each CTE

| # | CTE | Responsibility |
|---|---|---|
| 1 | `tb_transações` | Parsing dates, calculating `Diff_Date` (elapsed days) and transaction time |
| 2 | `tb_cliente` | Age in the database + flags for connected platforms |
| 3 | `tb_sumário_transações` | Count of transactions, balance and points by time window |
| 4 | `tb_transação_produto` | Enriches transactions with product name and category |
| 5 | `tb_cliente_produto` | Frequency of use by product and category, customer and window |
| 6 | `tb_cliente_produto_rn` | `ROW_NUMBER` for most used product in each window |
| 7 | `tb_cliente_dia` | Transactions by day of the week (D28) |
| 8 | `tb_cliente_dia_rn` | `ROW_NUMBER` for most active day |
| 9 | `tb_cliente_periodo` | Transactions by time of day (D28) |
| 10 | `tb_cliente_periodo_rn` | `ROW_NUMBER` for the most active time of day |
| 11 | `tb_join` | Consolidation of all CTEs into a single row per customer |

---

## ⚠️ Output Coverage

The database contains **4,962 customers**, but **1,469 (~30%) have never made a transaction**. As the output is based on `tb_sumário_transações` (which only returns rows for customers with at least one transaction), these inactive customers are **excluded from the final result**.

To include all customers (active and inactive), replace `tb_sumário_transações` as the base table and use `clientes` with LEFT JOINs.

---

## 🗃️ Schema of Source Tables

```sql
transacoes
├── IdTransacao          (PK)
├── IdCliente            (FK → clientes)
├── QtdePontos           (positivo = crédito | negativo = resgate)
├── DtCriacao
└── DescSistemaOrigem    ('twitch' | 'cursos')

clientes
├── idCliente            (PK)
├── qtdePontos           (saldo atual — equivale a SUM(transacoes.QtdePontos))
├── DtCriacao
├── DtAtualizacao
├── flTwitch             (1 = canal conectado)
├── flYouTube
├── flEmail
├── flBlueSky
└── flInstagram

transacao_produto
├── idTransacaoProduto   (PK)
├── IdTransacao          (FK → transacoes)
├── IdProduto            (FK → produtos)
├── QtdeProduto
└── vlProduto

produtos
├── IdProduto            (PK)
├── DescNomeProduto
├── DescDescricaoProduto
└── DescCategoriaProduto
```

## 💡 Technical Decisions

- **`julianday()`** — calculates the difference between dates in days in SQLite, without relying on external extensions.
- **`substr(DtCriacao, 1, 19)`** — removes milliseconds (`.114000`) and time zones from the `DtCriacao` column, ensuring compatibility with `datetime()` and `strftime()`.
- **`ROW_NUMBER()` with tie-breaking** — the secondary criterion `ORDER BY ... DESC, ProductName ASC` ensures deterministic results in the event of a tie between two products with the same frequency.
- **`NULLIF` in engagement** — `1.0 * D28 / NULLIF(Vida, 0)` prevents a division-by-zero error for customers with no history.
- **Consistent `COALESCE`** — `Day_Of_Week` uses `“N/A”` (text) rather than `-1` (integer), because `strftime(“%w”, ...)` returns TEXT in SQLite — mixing data types can cause unexpected behaviour in analytical tools.
- **Negative points as negative values** — facilitates direct balance calculation and the distinction between credit/debit without the need for extra columns.
- **Product category included** — in addition to the most commonly used product name, the query returns the category (`chat`, `present`, `espada`, `armadura`, etc.), useful for high-level segmentation.



