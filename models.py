import os

from peewee import *

database = PostgresqlDatabase(os.getenv('PG_DATABASE'), **{'host': os.getenv('PG_HOST'), 'user': os.getenv('PG_USER'), 'password': os.getenv('PG_PASSWORD')})

class UnknownField(object):
    def __init__(self, *_, **__): pass

class BaseModel(Model):
    class Meta:
        database = database

class Product(BaseModel):
    alcohol = BooleanField(null=True)
    amountperunit = FloatField(null=True)
    barcode = FloatField(null=True)
    barcodes = TextField(null=True)
    countryoforigin = TextField(null=True)
    currency = TextField(null=True)
    customernotesenabled = BooleanField(null=True)
    description = TextField(null=True)
    descriptionhtml = TextField(null=True)
    id = TextField(index=True, null=True)
    imageurl = TextField(null=True)
    imageurlbasename = TextField(null=True)
    maxquantity = FloatField(null=True)
    normalprice = FloatField(null=True)
    nutritionalinfo = TextField(null=True)
    packingsize = TextField(null=True)
    previewimageurl = TextField(null=True)
    price = FloatField(null=True)
    productbrand = TextField(null=True)
    productinfo = TextField(null=True)
    promotionendsat = TextField(null=True)
    promotionstartsat = TextField(null=True)
    size = TextField(null=True)
    slug = TextField(null=True)
    soldby = TextField(null=True)
    status = TextField(null=True)
    tags = TextField(null=True)
    title = TextField(null=True)
    unittype = TextField(null=True)
    brand_id = BigIntegerField(null=True)
    product_id = BigIntegerField(null=True)

    class Meta:
        table_name = 'products'
        primary_key = False

