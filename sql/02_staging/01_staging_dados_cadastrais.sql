CREATE TABLE staging_dados_cadastrais (
    id SERIAL PRIMARY KEY,
    REGISTRO_OPERADORA VARCHAR(50) NOT NULL,
    CNPJ VARCHAR(30) NOT NULL,
    Razao_Social VARCHAR(150) NOT NULL,
    Nome_Fantasia VARCHAR(150),
    Modalidade VARCHAR(100),
    Logradouro VARCHAR(250),
    Numero VARCHAR(20),
    Complemento VARCHAR(100),
    Bairro VARCHAR(100),
    Cidade VARCHAR(100),
    UF CHAR(2),
    CEP VARCHAR(20),
    DDD VARCHAR(5),
    Telefone VARCHAR(50),
    Fax VARCHAR(50),
    Endereco_eletronico VARCHAR(150),
    Representante VARCHAR(200),
    Cargo_Representante VARCHAR(200),
    Regiao_de_Comercializacao VARCHAR(15),
    Data_Registro_ANS DATE




);