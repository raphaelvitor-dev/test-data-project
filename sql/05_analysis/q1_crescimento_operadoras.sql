WITH despesas_trimestre AS (
    SELECT
        razao_social,
        ano,
        trimestre,
        SUM(valor_despesas) AS despesa
    FROM consolidado_despesas
    GROUP BY razao_social, ano, trimestre
),
limites AS (
    SELECT
        razao_social,
        MIN(
            CAST(ano AS int) * 10 +
            CAST(regexp_replace(trimestre, '\D', '', 'g') AS int)
        ) AS primeiro,
        MAX(
            CAST(ano AS int) * 10 +
            CAST(regexp_replace(trimestre, '\D', '', 'g') AS int)
        ) AS ultimo
    FROM despesas_trimestre
    GROUP BY razao_social
),
pivot AS (
    SELECT
        d.razao_social,
        MAX(d.despesa) FILTER (
            WHERE (
                CAST(d.ano AS int) * 10 +
                CAST(regexp_replace(d.trimestre, '\D', '', 'g') AS int)
            ) = l.primeiro
        ) AS despesa_inicial,
        MAX(d.despesa) FILTER (
            WHERE (
                CAST(d.ano AS int) * 10 +
                CAST(regexp_replace(d.trimestre, '\D', '', 'g') AS int)
            ) = l.ultimo
        ) AS despesa_final
    FROM despesas_trimestre d
    JOIN limites l
        ON d.razao_social = l.razao_social
    GROUP BY d.razao_social
)
SELECT
    razao_social,
    ((despesa_final - despesa_inicial) / despesa_inicial) * 100
        AS crescimento_percentual
FROM pivot
WHERE despesa_inicial > 0
ORDER BY crescimento_percentual DESC
LIMIT 5;
