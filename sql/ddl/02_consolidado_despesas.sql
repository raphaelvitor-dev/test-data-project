CREATE TABLE consolidado_despesas (
    id SERIAL PRIMARY KEY,
    data DATE,
    reg_ans VARCHAR(10) NOT NULL,
    cd_conta_contabil VARCHAR(20),
    descricao_gastos VARCHAR(200),
    vl_saldo_inicial DECIMAL(15,2),
    vl_saldo_final DECIMAL(15,2),
    valor_despesas DECIMAL(15,2),
    valor_suspeito BOOLEAN DEFAULT FALSE,
    ano SMALLINT,
    trimestre VARCHAR(2),
    cnpj CHAR(14) NOT NULL,
    razao_social VARCHAR(150) NOT NULL,
    nome_fantasia VARCHAR(150),
    modalidade VARCHAR(100),
    logradouro VARCHAR(250),
    numero VARCHAR(20),
    complemento VARCHAR(100),
    bairro VARCHAR(100),
    cidade VARCHAR(100),
    uf CHAR(2),
    cep CHAR(8),
    ddd CHAR(2),
    telefone VARCHAR(15),
    fax VARCHAR(15),
    endereco_eletronico VARCHAR(150),
    representante VARCHAR(200),
    cargo_representante VARCHAR(50),
    regiao_comercializacao SMALLINT,
    data_registro_ans DATE,

    CONSTRAINT chk_uf_completos CHECK (uf IS NULL OR uf ~ '^[A-Z]{2}$'),
    CONSTRAINT chk_cep_completos CHECK (cep IS NULL OR cep ~ '^[0-9]{8}$'),
    CONSTRAINT chk_ddd_completos CHECK (ddd IS NULL OR ddd ~ '^[0-9]{2}$'),
    CONSTRAINT chk_email_completos CHECK (endereco_eletronico IS NULL OR endereco_eletronico ~* '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
    CONSTRAINT chk_trimestre CHECK (trimestre IS NULL OR trimestre IN ('T1', 'T2', 'T3', 'T4', '1T', '2T', '3T', '4T', '1', '2', '3', '4')),
    CONSTRAINT chk_ano CHECK (ano IS NULL OR ano BETWEEN 1900 AND 2100),
    CONSTRAINT chk_regiao CHECK (regiao_comercializacao IS NULL OR regiao_comercializacao BETWEEN 1 AND 5),


    CONSTRAINT fk_cnpj FOREIGN KEY (cnpj)
        REFERENCES dados_cadastrais(cnpj)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,

    CONSTRAINT fk_reg_ans FOREIGN KEY (reg_ans)
        REFERENCES dados_cadastrais(reg_op)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
);