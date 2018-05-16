# coding: utf-8
import os

import psycopg2

conn = psycopg2.connect("postgresql://{}:{}@{}/{}".format(
    os.getenv("PG_USER"),
    os.getenv("PG_PASSWORD"),
    os.getenv("PG_HOST"),
    os.getenv("PG_DATABASE"),
    ))
cur = conn.cursor()

cur.execute("""
        DROP TABLE IF EXISTS products CASCADE;

CREATE TABLE products 
(
   id                    text,
   alcohol               boolean,
   amountperunit         float8,
   barcode               float8,
   barcodes              text,
   countryoforigin       text,
   currency              text,
   customernotesenabled  boolean,
   description           text,
   descriptionhtml       text,
   imageurl              text,
   imageurlbasename      text,
   maxquantity           float8,
   normalprice           float8,
   nutritionalinfo       text,
   packingsize           text,
   previewimageurl       text,
   price                 float8,
   productbrand          text,
   productinfo           text,
   promotionendsat       text,
   promotionstartsat     text,
   size                  text,
   slug                  text,
   soldby                text,
   status                text,
   tags                  text,
   title                 text,
   store_id              bigint,
   product_id            bigint,
   category_id           bigint,
   department_id         bigint,
   url                   text,
   dt                    timestamptz,
   unittype              text
);


CREATE INDEX ix_honestbee_crawler_id ON public.products USING btree (id);


COMMIT;


        """)
# ALTER TABLE products
#    ADD CONSTRAINT pk_products
#    PRIMARY KEY (id);
