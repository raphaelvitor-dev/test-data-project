CREATE TABLE despesas_consolidadas (
    id SERIAL NOT NULL PRIMARY KEY,
    data DATE,
    reg_ans NOT NULL VARCHAR(10),
    descricao_gastos VARCHAR(100),
    vl_saldo_inicial DECIMAL,
    vl_saldo_final DECIMAL,
    valor_despesas DECIMAL,
    valor_suspeito BOOLEAN,
    ano INT
)