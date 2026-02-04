WITH despesas_trimestrais AS (
    SELECT
        razao_social,
        ano,
        trimestre,
        SUM(valor_despesas) AS total_trimestre
    FROM consolidado_despesas
    GROUP BY razao_social, ano, trimestre
),
media_trimestre AS (
    SELECT
        ano,
        trimestre,
        AVG(total_trimestre) AS media_geral
    FROM despesas_trimestrais
    GROUP BY ano, trimestre
),
acima_media AS (
    SELECT
        d.razao_social,
        COUNT(*) AS trimestres_acima_media
    FROM despesas_trimestrais d
    JOIN media_trimestre m
      ON d.ano = m.ano
     AND d.trimestre = m.trimestre
    WHERE d.total_trimestre > m.media_geral
    GROUP BY d.razao_social
)
SELECT
    COUNT(*) AS qtd_operadoras
FROM acima_media
WHERE trimestres_acima_media >= 2;
