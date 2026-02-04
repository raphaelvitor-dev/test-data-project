WITH despesas_operadora_uf AS (
    SELECT
        uf,
        razao_social,
        SUM(valor_despesas) AS total_operadora_uf
    FROM consolidado_despesas
    GROUP BY uf, razao_social
),
agregado_uf AS (
    SELECT
        uf,
        SUM(total_operadora_uf) AS total_despesas_uf,
        AVG(total_operadora_uf) AS media_por_operadora
    FROM despesas_operadora_uf
    GROUP BY uf
)
SELECT
    uf,
    total_despesas_uf,
    media_por_operadora
FROM agregado_uf
ORDER BY total_despesas_uf DESC
LIMIT 5;
