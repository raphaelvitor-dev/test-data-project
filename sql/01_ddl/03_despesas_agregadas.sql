CREATE TABLE despesas_agregadas (
    id SERIAL PRIMARY KEY,
    razao_social VARCHAR(150) NOT NULL,
    uf CHAR(2),
    total_despesas DECIMAL(15,2),
    media_trimestral DECIMAL(15,2),
    desvio_padrao DECIMAL(15,2)


);

