
COPY staging_despesas_agregadas(
    razaosocial,
    uf,
    totaldespesas,
    mediatrimestral,
    desviopadrao
    )
FROM 'F:\PYTHON\test-data-project\Trimestres\despesas_agregadas.csv'
DELIMITER ';'
CSV HEADER
ENCODING 'UTF8';


INSERT INTO despesas_agregadas (
    razao_social,
    uf,
    total_despesas,
    media_trimestral,
    desvio_padrao
)
SELECT
    razaosocial,
    uf,
    totaldespesas,
    mediatrimestral,
    desviopadrao
FROM staging_despesas_agregadas;

TRUNCATE TABLE staging_consolidado_despesas;
TRUNCATE TABLE staging_dados_cadastrais;
TRUNCATE TABLE staging_despesas_agregadas;

