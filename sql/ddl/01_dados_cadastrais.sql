CREATE TABLE dados_cadastrais (
    id SERIAL PRIMARY KEY,
    reg_op VARCHAR(10) NOT NULL,
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

    CONSTRAINT chk_uf CHECK (uf ~ '^[A-Z]{2}$'),
    CONSTRAINT chk_cep CHECK (cep IS NULL OR cep ~ '^[0-9]{8}$'),
    CONSTRAINT chk_ddd CHECK (ddd IS NULL OR ddd ~ '^[0-9]{2}$'),
    CONSTRAINT chk_email CHECK (endereco_eletronico IS NULL OR endereco_eletronico ~* '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
    CONSTRAINT chk_regiao_cadastral CHECK (regiao_comercializacao IS NULL OR regiao_comercializacao BETWEEN 1 AND 5),

    CONSTRAINT cnpj_unico UNIQUE (cnpj),
    CONSTRAINT reg_op_unico UNIQUE (reg_op)
);

CREATE INDEX idx_cnpj ON dados_cadastrais(cnpj);
CREATE INDEX idx_reg_op ON dados_cadastrais(reg_op);