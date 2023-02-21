CREATE TABLE CURRENCY_BAL
(
 account_name      VARCHAR(13) NOT NULL,
 contract          VARCHAR(13) NOT NULL,
 currency          VARCHAR(8) NOT NULL,
 amount            DECIMAL(22,0) NOT NULL,
 decimals          SMALLINT NOT NULL
);

CREATE UNIQUE INDEX CURRENCY_BAL_I01 ON CURRENCY_BAL (account_name, contract, currency);
CREATE INDEX CURRENCY_BAL_I02 ON CURRENCY_BAL (contract, currency, amount);
CREATE INDEX CURRENCY_BAL_I03 ON CURRENCY_BAL (currency, contract);

