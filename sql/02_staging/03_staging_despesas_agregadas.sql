CREATE TABLE staging_despesas_agregadas (
    id SERIAL PRIMARY KEY,
    razaosocial VARCHAR(150) NOT NULL,
    uf CHAR(2),
    totaldespesas DECIMAL(15,2),
    mediatrimestral DECIMAL(15,2),
    desviopadrao DECIMAL(15,2)


);

