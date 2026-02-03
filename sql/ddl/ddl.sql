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


CREATE TABLE despesas_agregadas (
    id SERIAL PRIMARY KEY,

    razao_social VARCHAR(150) NOT NULL,
    uf CHAR(2),

    total_despesas DECIMAL(15,2),
    media_trimestral DECIMAL(15,2),
    desvio_padrao DECIMAL(15,2),

    CONSTRAINT chk_uf_agregados CHECK (uf IS NULL OR uf ~ '^[A-Z]{2}$'),
    CONSTRAINT chk_total_despesas CHECK (total_despesas IS NULL OR total_despesas >= 0),
    CONSTRAINT chk_media_trimestral CHECK (media_trimestral IS NULL OR media_trimestral >= 0),
    CONSTRAINT chk_desvio_padrao CHECK (desvio_padrao IS NULL OR desvio_padrao >= 0)
);

CREATE INDEX idx_razao_social_agregados ON despesas_agregadas(razao_social);
CREATE INDEX idx_uf_agregados ON despesas_agregadas(uf);


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

CREATE INDEX idx_data_completos ON consolidado_despesas(data);
CREATE INDEX idx_reg_ans_completos ON consolidado_despesas(reg_ans);
CREATE INDEX idx_cnpj_completos ON consolidado_despesas(cnpj);
CREATE INDEX idx_razao_social_completos ON consolidado_despesas(razao_social);
CREATE INDEX idx_uf_completos ON consolidado_despesas(uf);
CREATE INDEX idx_ano_trimestre ON consolidado_despesas(ano, trimestre);
CREATE INDEX idx_valor_suspeito ON consolidado_despesas(valor_suspeito) WHERE valor_suspeito = TRUE;
CREATE INDEX idx_periodo_operadora ON consolidado_despesas(ano, trimestre, reg_ans);

--COMENTÁRIOS SOBRE OS TRADE-OFFS--
--Os dados das tabelas que irão enriquecer o DB, já vem em sua maioria tratado.