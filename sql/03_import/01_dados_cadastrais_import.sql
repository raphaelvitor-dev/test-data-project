COPY staging_dados_cadastrais(
    REGISTRO_OPERADORA,
    CNPJ,
    Razao_Social,
    Nome_Fantasia,
    Modalidade,
    Logradouro,
    Numero,
    Complemento,
    Bairro,
    Cidade,
    UF,
    CEP,
    DDD,
    Telefone,
    Fax,
    Endereco_eletronico,
    Representante,
    Cargo_Representante,
    Regiao_de_Comercializacao,
    Data_Registro_ANS
)

FROM 'F:/PYTHON/test-data-project/Trimestres/Relatorio_cadop.csv'
DELIMITER ';'
CSV HEADER
QUOTE '"'
ENCODING 'UTF8';


-- Insere na tabela final com tratamento de NULL e filtragem
INSERT INTO dados_cadastrais (
    reg_op,
    cnpj,
    razao_social,
    nome_fantasia,
    modalidade,
    logradouro,
    numero,
    complemento,
    bairro,
    cidade,
    uf,
    cep,
    ddd,
    telefone,
    fax,
    endereco_eletronico,
    representante,
    cargo_representante,
    regiao_comercializacao,
    data_registro_ans
)
SELECT
    REGISTRO_OPERADORA,
    CNPJ,
    COALESCE(Razao_Social,'SEM NOME'),
    Nome_Fantasia,
    Modalidade,
    Logradouro,
    Numero,
    Complemento,
    Bairro,
    Cidade,
    UF,
    CEP,
    DDD,
    Telefone,
    Fax,
    Endereco_eletronico,
    Representante,
    Cargo_Representante,
    Regiao_de_Comercializacao,
    CAST(Data_Registro_ANS AS DATE)
FROM staging_dados_cadastrais;

