-- ============================================================
-- USER BEHAVIORAL PROFILE
-- Programa de Pontos — Plataforma Twitch / Cursos
-- ============================================================
-- Objetivo: Construir uma tabela analítica com o perfil
-- comportamental completo de cada usuário.
--
-- Métricas geradas:
--   • Quantidade de transações         (Vida, D7, D14, D28, D56)
--   • Dias desde a última transação    (recência)
--   • Idade na base                    (dias desde o cadastro)
--   • Produto mais usado               (Vida, D7, D14, D28, D56)
--   • Categoria do produto mais usado  (Vida, D7, D14, D28, D56)
--   • Saldo de pontos atual
--   • Pontos acumulados positivos      (Vida, D7, D14, D28, D56)
--   • Pontos acumulados negativos      (Vida, D7, D14, D28, D56)
--   • Dia da semana mais ativo         (D28)
--   • Período do dia mais ativo        (D28)
--   • Engajamento D28 vs. Vida
--   • Plataformas conectadas           (Twitch, YouTube, Email, etc.)
--
-- Compatibilidade: SQLite
--
-- ⚠️  Nota importante sobre cobertura:
--     A base possui 4.962 clientes registados, dos quais 1.469
--     (~30%) nunca realizaram transações. Como o output parte de
--     tb_sumário_transações (que agrega apenas clientes com ao
--     menos 1 transação), estes clientes inativos são EXCLUÍDOS
--     do resultado final. Para incluí-los, o ponto de partida
--     deve ser a tabela `clientes` com LEFT JOINs.
-- ============================================================


WITH

-- ------------------------------------------------------------
-- 1. tb_transações
-- Limpeza e enriquecimento da tabela de transações brutas.
--
-- • substr(DtCriacao, 1, 19) → remove milissegundos e fusos,
--   garantindo formato 'YYYY-MM-DD HH:MM:SS' compatível com
--   as funções datetime() e strftime() do SQLite.
-- • Diff_Date: dias corridos desde a transação (float).
--   Valores próximos de 0 = transações recentes.
-- • Dt_Hora: hora extraída como integer para classificação
--   de período do dia na CTE tb_cliente_periodo.
-- ------------------------------------------------------------
tb_transações AS (

    SELECT
        IdTransacao,
        IdCliente,
        QtdePontos,
        datetime(substr(DtCriacao, 1, 19))                          AS Dt_Criação,
        julianday('now') - julianday(substr(DtCriacao, 1, 10))      AS Diff_Date,
        CAST(strftime('%H', substr(DtCriacao, 1, 19)) AS INTEGER)   AS Dt_Hora

    FROM transacoes
),


-- ------------------------------------------------------------
-- 2. tb_cliente
-- Extrai dados de cadastro e flags de plataforma de cada cliente.
--
-- • Idade_Base: dias corridos desde o cadastro. Permite
--   segmentar clientes novos vs. maduros.
-- • Flags de plataforma (1 = conectado, 0 = não conectado):
--   flTwitch (principal, ~69% dos clientes), flEmail (~4%),
--   flYouTube (~3%), flBlueSky e flInstagram (ainda não usados).
-- ------------------------------------------------------------
tb_cliente AS (

    SELECT
        idCliente,
        julianday('now') - julianday(substr(DtCriacao, 1, 10))      AS Idade_Base,
        flTwitch,
        flYouTube,
        flEmail,
        flBlueSky,
        flInstagram

    FROM clientes
),


-- ------------------------------------------------------------
-- 3. tb_sumário_transações
-- Agregação central por cliente — todas as métricas de volume,
-- recência e pontos segmentadas por janela temporal.
--
-- • Janelas: Vida (histórico completo), D56, D28, D14, D7.
-- • Dias_Ultima_Interação: MIN(Diff_Date) = transação mais
--   recente. Quanto menor, mais recente o cliente.
-- • Saldo_Pontos: soma de todos os movimentos (positivo =
--   crédito, negativo = resgate). Validado contra
--   clientes.qtdePontos — os valores batem exatamente.
-- • Pontos negativos são mantidos como valores negativos,
--   facilitando a distinção crédito/débito na análise.
--   Na base atual, resgates representam < 1% das transações.
-- ------------------------------------------------------------
tb_sumário_transações AS (

    SELECT
        IdCliente,

        -- Volume de transações por janela temporal
        COUNT(IdTransacao)                                                               AS Qt_Transações_Vida,
        COUNT(CASE WHEN Diff_Date <= 56 THEN IdTransacao END)                           AS Qt_Transações_D56,
        COUNT(CASE WHEN Diff_Date <= 28 THEN IdTransacao END)                           AS Qt_Transações_D28,
        COUNT(CASE WHEN Diff_Date <= 14 THEN IdTransacao END)                           AS Qt_Transações_D14,
        COUNT(CASE WHEN Diff_Date <=  7 THEN IdTransacao END)                           AS Qt_Transações_D7,

        -- Recência: distância em dias da última interação
        MIN(Diff_Date)                                                                   AS Dias_Ultima_Interação,

        -- Saldo atual = créditos acumulados − resgates
        SUM(QtdePontos)                                                                  AS Saldo_Pontos,

        -- Créditos (pontos recebidos) por janela
        SUM(CASE WHEN QtdePontos >  0                     THEN QtdePontos ELSE 0 END)   AS Pontos_Positivos_Vida,
        SUM(CASE WHEN QtdePontos >  0 AND Diff_Date <= 56 THEN QtdePontos ELSE 0 END)   AS Pontos_Positivos_D56,
        SUM(CASE WHEN QtdePontos >  0 AND Diff_Date <= 28 THEN QtdePontos ELSE 0 END)   AS Pontos_Positivos_D28,
        SUM(CASE WHEN QtdePontos >  0 AND Diff_Date <= 14 THEN QtdePontos ELSE 0 END)   AS Pontos_Positivos_D14,
        SUM(CASE WHEN QtdePontos >  0 AND Diff_Date <=  7 THEN QtdePontos ELSE 0 END)   AS Pontos_Positivos_D7,

        -- Débitos (resgates de pontos) por janela — valores negativos
        SUM(CASE WHEN QtdePontos <  0                     THEN QtdePontos ELSE 0 END)   AS Pontos_Negativos_Vida,
        SUM(CASE WHEN QtdePontos <  0 AND Diff_Date <= 56 THEN QtdePontos ELSE 0 END)   AS Pontos_Negativos_D56,
        SUM(CASE WHEN QtdePontos <  0 AND Diff_Date <= 28 THEN QtdePontos ELSE 0 END)   AS Pontos_Negativos_D28,
        SUM(CASE WHEN QtdePontos <  0 AND Diff_Date <= 14 THEN QtdePontos ELSE 0 END)   AS Pontos_Negativos_D14,
        SUM(CASE WHEN QtdePontos <  0 AND Diff_Date <=  7 THEN QtdePontos ELSE 0 END)   AS Pontos_Negativos_D7

    FROM tb_transações
    GROUP BY IdCliente
),


-- ------------------------------------------------------------
-- 4. tb_transação_produto
-- Enriquece cada transação com nome e categoria do produto.
--
-- Estrutura do join:
--   transacoes ──(1:N)── transacao_produto ──(N:1)── produtos
--
-- Cobertura do join: 99,86% das linhas têm produto identificado.
-- Os ~0,14% sem match correspondem a IdProduto vazio ('').
--
-- Os produtos dividem-se em dois grupos:
--   1. Ações de engajamento: 'ChatMessage', 'Lista de presença',
--      'Presença Streak' — representam > 97% do volume.
--   2. Itens RPG (espadas, armaduras, etc.) — transações de
--      resgate, onde o cliente gasta pontos acumulados.
-- ------------------------------------------------------------
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


-- ------------------------------------------------------------
-- 5. tb_cliente_produto
-- Conta quantas vezes cada cliente interagiu com cada produto,
-- por janela temporal.
-- Inclui DescCategoriaProduto para enriquecer o perfil final
-- além do nome do produto mais usado.
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


-- ------------------------------------------------------------
-- 6. tb_cliente_produto_rn
-- Ranqueia produtos por frequência de uso em cada janela.
-- RN = 1 identifica o produto mais utilizado.
--
-- ⚠️  Em caso de empate, ROW_NUMBER() desempata por
--     DescNomeProduto ASC para garantir determinismo.
-- ------------------------------------------------------------
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


-- ------------------------------------------------------------
-- 7. tb_cliente_dia
-- Conta transações por dia da semana nos últimos 28 dias.
--
-- strftime('%w', ...) retorna TEXT:
--   '0'=Domingo | '1'=Segunda | '2'=Terça | '3'=Quarta
--   '4'=Quinta  | '5'=Sexta   | '6'=Sábado
-- ------------------------------------------------------------
tb_cliente_dia AS (

    SELECT
        IdCliente,
        strftime('%w', Dt_Criação)  AS Dia_Semana,
        COUNT(*)                    AS Qt_Transações

    FROM tb_transações
    WHERE Diff_Date <= 28
    GROUP BY IdCliente, Dia_Semana
),


-- ------------------------------------------------------------
-- 8. tb_cliente_dia_rn
-- Ranqueia os dias da semana por volume de transações.
-- RN = 1 → dia mais ativo nos últimos 28 dias.
-- Desempate por Dia_Semana ASC para determinismo.
-- ------------------------------------------------------------
tb_cliente_dia_rn AS (

    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY IdCliente ORDER BY Qt_Transações DESC, Dia_Semana) AS RN_Dia

    FROM tb_cliente_dia
),


-- ------------------------------------------------------------
-- 9. tb_cliente_periodo
-- Classifica transações dos últimos 28 dias por período do dia
-- e conta o total por cliente e período.
--
-- Faixas horárias definidas:
--   Manhã:     07h – 12h
--   Tarde:     13h – 18h
--   Noite:     19h – 23h
--   Madrugada: 00h – 06h
-- ------------------------------------------------------------
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


-- ------------------------------------------------------------
-- 10. tb_cliente_periodo_rn
-- Ranqueia períodos por volume de transações.
-- RN = 1 → período mais ativo nos últimos 28 dias.
-- ------------------------------------------------------------
tb_cliente_periodo_rn AS (

    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY IdCliente ORDER BY Qt_Transações DESC, Periodo) AS RN_Periodo

    FROM tb_cliente_periodo
),


-- ------------------------------------------------------------
-- 11. tb_join
-- Consolida todas as CTEs em uma única linha por cliente.
--
-- COALESCE garante que clientes sem atividade nos últimos 28
-- dias não gerem NULLs no output:
--   • Dia_Semana_D28: 'N/A' (texto, consistente com strftime)
--   • Periodo_D28:    'Sem Informação'
-- ------------------------------------------------------------
tb_join AS (

    SELECT
        t1.*,

        -- Dados cadastrais e plataformas conectadas
        t2.Idade_Base,
        t2.flTwitch,
        t2.flYouTube,
        t2.flEmail,
        t2.flBlueSky,
        t2.flInstagram,

        -- Produto + categoria mais usados por janela temporal
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

        -- Dia da semana mais ativo em D28
        -- '0'=Dom '1'=Seg '2'=Ter '3'=Qua '4'=Qui '5'=Sex '6'=Sáb
        COALESCE(t8.Dia_Semana, 'N/A')          AS Dia_Semana_Mais_Ativo_D28,

        -- Período do dia mais ativo em D28
        COALESCE(t9.Periodo, 'Sem Informação')  AS Periodo_Mais_Ativo_D28

    FROM tb_sumário_transações AS t1

    LEFT JOIN tb_cliente AS t2
        ON t1.IdCliente = t2.idCliente

    -- Produto mais usado — Vida
    LEFT JOIN tb_cliente_produto_rn AS t3
        ON t1.IdCliente = t3.IdCliente AND t3.RN_Vida = 1

    -- Produto mais usado — D56
    LEFT JOIN tb_cliente_produto_rn AS t4
        ON t1.IdCliente = t4.IdCliente AND t4.RN_D56  = 1

    -- Produto mais usado — D28
    LEFT JOIN tb_cliente_produto_rn AS t5
        ON t1.IdCliente = t5.IdCliente AND t5.RN_D28  = 1

    -- Produto mais usado — D14
    LEFT JOIN tb_cliente_produto_rn AS t6
        ON t1.IdCliente = t6.IdCliente AND t6.RN_D14  = 1

    -- Produto mais usado — D7
    LEFT JOIN tb_cliente_produto_rn AS t7
        ON t1.IdCliente = t7.IdCliente AND t7.RN_D7   = 1

    -- Dia mais ativo em D28
    LEFT JOIN tb_cliente_dia_rn AS t8
        ON t1.IdCliente = t8.IdCliente AND t8.RN_Dia  = 1

    -- Período mais ativo em D28
    LEFT JOIN tb_cliente_periodo_rn AS t9
        ON t1.IdCliente = t9.IdCliente AND t9.RN_Periodo = 1
)


-- ============================================================
-- OUTPUT FINAL
-- Uma linha por cliente com todas as métricas.
--
-- Engajamento_D28_Vida:
--   Proporção das transações D28 sobre o histórico total.
--   Range: 0.0 (inativo em D28) → 1.0 (toda atividade em D28).
--   NULLIF evita divisão por zero para clientes sem histórico.
-- ============================================================
SELECT
    *,
    1.0 * Qt_Transações_D28 / NULLIF(Qt_Transações_Vida, 0)  AS Engajamento_D28_Vida

FROM tb_join;
