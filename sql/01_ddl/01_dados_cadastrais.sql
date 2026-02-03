CREATE TABLE dados_cadastrais (
    id SERIAL PRIMARY KEY,
    reg_op VARCHAR(50) NOT NULL,
    cnpj VARCHAR(30) NOT NULL,
    razao_social VARCHAR(150) NOT NULL,
    nome_fantasia VARCHAR(150),
    modalidade VARCHAR(100),
    logradouro VARCHAR(250),
    numero VARCHAR(20),
    complemento VARCHAR(100),
    bairro VARCHAR(100),
    cidade VARCHAR(100),
    uf CHAR(2),
    cep VARCHAR(20),
    ddd VARCHAR(5),
    telefone VARCHAR(50),
    fax VARCHAR(50),
    endereco_eletronico VARCHAR(150),
    representante VARCHAR(200),
    cargo_representante VARCHAR(200),
    regiao_comercializacao VARCHAR(15),
    data_registro_ans DATE,


    CONSTRAINT cnpj_unico UNIQUE (cnpj),
    CONSTRAINT reg_op_unico UNIQUE (reg_op)
);

