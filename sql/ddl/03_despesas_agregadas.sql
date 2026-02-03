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