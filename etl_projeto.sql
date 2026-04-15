-- Vamos construir uma tabela com o perfil comportamental dos nossos usuários.
-- Quantidade de transações históricas (vida, D7, D14, D28, D56);
-- Dias desde a última transação
-- Idade na base (quando a pessoa se cadastrou)
-- Produto mais usado (vida, D7, D14, D28, D56);
-- Saldo de pontos atual;
-- Pontos acumulados positivos (vida, D7, D14, D28, D56);
-- Pontos acumulados negativos (vida, D7, D14, D28, D56);
-- Dias da semana mais ativos (D28)
-- Período do dia mais ativo (D28)
-- Engajamento em D28 versus Vida

with tb_transações as (
    
    SELECT IdTransacao,
           idCliente,
           qtdePontos,
           datetime(substr(DtCriacao,1,19)) as Dt_Criação,
           julianday("now") - julianday(substr(DtCriacao,1,10)) as  Diff_Date,
           cast(strftime("%H", substr(DtCriacao,1,19)) as Integer) as Dt_Hora

    From transacoes
),

tb_cliente as (
    SELECT idCliente,
           datetime(substr(DtCriacao,1,19)) as Dt_Criação,
           julianday("now") - julianday(substr(DtCriacao,1,10)) as Idade_Base
    
    From clientes
),

tb_sumário_transações as (

    SELECT idCliente,
        count(IdTransacao) as Qt_de_Transações_Vida,
        count(case when Diff_Date <= 56 then IdTransacao end) as Qt_de_Transações56,
        count(case when Diff_Date <= 28 then IdTransacao end) as Qt_de_Transações28,
        count(case when Diff_Date <= 14 then IdTransacao end) as Qt_de_Transações14,
        count(case when Diff_Date <= 7 then IdTransacao end) as Qt_de_Transações07,

        min(Diff_Date) as Dias_Ultima_Interação,

        sum(QtdePontos) as Saldo_Pontos,

        sum(case when qtdePontos > 0 then QtdePontos else 0 end) Qt_de_Pontos_Positivos_Vida,
        sum(case when qtdePontos > 0 and Diff_Date <= 56 then qtdePontos else 0 end) as Qt_de_Pontos_Positivos_Via56,
        sum(case when qtdePontos > 0 and Diff_Date <= 28 then qtdePontos else 0 end) as Qt_de_Pontos_Positivos_Via28,
        sum(case when qtdePontos > 0 and Diff_Date <= 14 then qtdePontos else 0 end) as Qt_de_Pontos_Positivos_Via14,
        sum(case when qtdePontos > 0 and Diff_Date <= 7 then qtdePontos else 0 end) as Qt_de_Pontos_Positivos_Via7,

        sum(case when qtdePontos < 0 then QtdePontos else 0 end) Qt_de_Pontos_Negativos_Vida,
        sum(case when qtdePontos < 0 and Diff_Date <= 56 then qtdePontos else 0 end) as Qt_de_Pontos_Negativos_Via56,
        sum(case when qtdePontos < 0 and Diff_Date <= 28 then qtdePontos else 0 end) as Qt_de_Pontos_Negativos_Via28,
        sum(case when qtdePontos < 0 and Diff_Date <= 14 then qtdePontos else 0 end) as Qt_de_Pontos_Negativos_Via14,
        sum(case when qtdePontos < 0 and Diff_Date <= 7 then qtdePontos else 0 end) as Qt_de_Pontos_Negativos_Via7

    From tb_transações
    Group by idCliente
),


tb_transação_produto as (

    SELECT t1.*,
        t3.DescNomeProduto,
        t3.DescCategoriaProduto 

    From tb_transações as t1

    left join transacao_produto as t2
    on t1.IdTransacao = t2.IdTransacao

    left JOIN produtos as t3
    on t2.IdProduto = t3.IdProduto
),

tb_cliente_produto as (

SELECT idCliente,
       DescNomeProduto,
       count(*) as Qt_de_Vida,
       count( case when Diff_Date <= 56 then IdTransacao end) as Qt_de_Vida56,
       count( case when Diff_Date <= 28 then IdTransacao end) as Qt_de_Vida28,
       count( case when Diff_Date <= 14 then IdTransacao end) as Qt_de_Vida14,
       count( case when Diff_Date <= 7 then IdTransacao end) as Qt_de_Vida07

From  tb_transação_produto

Group by idCliente, DescNomeProduto
),

tb_cliente_produto_rn as (

    SELECT *,
        row_number() over (PARTITION by idCliente order by Qt_de_Vida desc) as RN_Vida,
        row_number() over (PARTITION by idCliente order by Qt_de_Vida56 desc) as RN_Vida56,
        row_number() over (PARTITION by idCliente order by Qt_de_Vida28 desc) as RN_Vida28,
        row_number() over (PARTITION by idCliente order by Qt_de_Vida14 desc) as RN_Vida14,
        row_number() over (PARTITION by idCliente order by Qt_de_Vida07 desc) as RN_Vida07

    From tb_cliente_produto
),

tb_cliente_dia as (

SELECT idCliente,
       strftime("%w", Dt_Criação) as Dt_Dia,
       count(*) as Qt_de_Transação
From tb_transações
where Diff_Date <= 28
Group by idCliente, Dt_dia
),

tb_cliente_dia_rn as (

SELECT *,
       row_number () over (PARTITION by idCliente order by Qt_de_Transação desc) as RN_Dia

From tb_cliente_dia
),

tb_cliente_periodo as (

    SELECT
        idCliente,
        case
            when Dt_Hora BETWEEN 7 and 12 then "Manhã"
            when Dt_Hora BETWEEN 13 and 18 then "Tarde"
            when Dt_Hora BETWEEN 19 and 23 then "Noite"
            Else "Madrugada"
            end as Periodo,
            count(*) as Qt_de_Transação


    From tb_transações
    where Diff_Date <= 28

    group by 1,2
),

tb_cliente_periodo_rn as (

SELECT *,
       row_number() over (PARTITION by idCliente ORDER by Qt_de_Transação desc) as RN_Periodo

From tb_cliente_periodo
),


tb_join as (

    SELECT t1.*,
        t2.Idade_Base,
        t3.DescNomeProduto as Produto_Vida,
        t4.DescNomeProduto as Produto_Vida56,
        t5.DescNomeProduto as Produto_Vida28,
        t6.DescNomeProduto as Produto_Vida14,
        t7.DescNomeProduto as Produto_Vida07,
        coalesce(t8.Dt_Dia, -1) as Dt_Dia,
        coalesce(t9.periodo, "Sem Informação") as Periodo_28

    From tb_sumário_transações as t1

    left join tb_cliente as t2
    on t1.idCliente = t2.idCliente
    
    left join tb_cliente_produto_rn as t3
    on t1.idCliente = t3.idCliente
    and t3.RN_Vida = 1

    left join tb_cliente_produto_rn as t4
    on t1.idCliente = t4.idCliente
    and t4.RN_Vida56 = 1

    left join tb_cliente_produto_rn as t5
    on t1.idCliente = t5.idCliente
    and t5.RN_Vida28 = 1

    left join tb_cliente_produto_rn as t6
    on t1.idCliente = t6.idCliente
    and t6.RN_Vida14 = 1

    left join tb_cliente_produto_rn as t7
    on t1.idCliente = t7.idCliente
    and t7.RN_Vida07 = 1

    left join tb_cliente_dia_rn as t8
    on t1.idCliente = t8.idCliente
    and t8.RN_Dia = 1

    left join tb_cliente_periodo_rn as t9
    on t1.idCliente = t9.idCliente
    and t9.RN_Periodo = 1

)


SELECT *,
       1. *Qt_de_Transações28 / Qt_de_Transações_Vida as Engajamento_28_Vida


From tb_join
