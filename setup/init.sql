-- 데이터베이스 생성
CREATE DATABASE news;

-- 데이터베이스 연결
\c news;

-- pgvector 확장 설치
CREATE EXTENSION IF NOT EXISTS vector;

-- 뉴스 기사 테이블 생성
CREATE TABLE news_article(
    id SERIAL NOT NULL,
    title varchar(200) NOT NULL,
    writer varchar(255) NOT NULL,
    write_date timestamp without time zone NOT NULL,
    category varchar(50) NOT NULL,
    content text NOT NULL,
    url varchar(200) NOT NULL,
    keywords jsonb DEFAULT '[]'::json,
    embedding vector NOT NULL,
    updated_at timestamp without time zone DEFAULT now(),
    PRIMARY KEY(id)
);
CREATE UNIQUE INDEX news_article_url_key ON public.news_article USING btree (url);