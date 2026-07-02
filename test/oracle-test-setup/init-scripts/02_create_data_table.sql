-- Tabel target untuk uji skala besar (~1M+ baris) menggunakan
-- data hasil generate_test_data.py
ALTER SESSION SET CONTAINER = XEPDB1;

ALTER SESSION SET CURRENT_SCHEMA = testuser;

CREATE TABLE data (
    id           NUMBER PRIMARY KEY,
    first_name   VARCHAR2(50),
    last_name    VARCHAR2(50),
    email        VARCHAR2(100),
    phone        VARCHAR2(30),
    address      VARCHAR2(100),
    city         VARCHAR2(50),
    country      VARCHAR2(10),
    postal_code  VARCHAR2(10),
    company      VARCHAR2(50),
    job_title    VARCHAR2(50),
    department   VARCHAR2(50),
    status       VARCHAR2(20),
    category     VARCHAR2(20),
    amount       NUMBER(14,2),
    score        NUMBER(9,4),
    description  VARCHAR2(600),
    notes        VARCHAR2(250),
    metadata     VARCHAR2(200),
    created_at   TIMESTAMP,
    updated_at   TIMESTAMP
);
