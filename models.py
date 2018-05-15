import os

from peewee import *
from playhouse.postgres_ext import DateTimeTZField, JSONField

database = PostgresqlDatabase(os.getenv('PG_DATABASE'), **{'host': os.getenv('PG_HOST'), 'user': os.getenv('PG_USER'), 'password': os.getenv('PG_PASSWORD')})

class UnknownField(object):
    def __init__(self, *_, **__): pass

class BaseModel(Model):
    class Meta:
        database = database

class Product(BaseModel):
    url = TextField(null=True)
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
    dt = DateTimeTZField(null=True)

    class Meta:
        table_name = 'products'
        primary_key = False

class Brands(BaseModel):
    about = TextField(null=True)
    brandcolor = TextField(null=True)
    brandtraits = JSONField(null=True)
    brandtype = TextField(null=True)
    cashbackamount = BigIntegerField(null=True)
    closed = TextField(null=True)
    currency = TextField(null=True)
    defaultconciergefee = BigIntegerField(null=True)
    defaultdeliveryfee = BigIntegerField(null=True)
    deliverytypes = TextField(null=True)
    description = TextField(null=True)
    estimateddeliverytime = TextField(null=True)
    freedeliveryamount = BigIntegerField(null=True)
    freedeliveryeligible = BooleanField(null=True)
    id = BigIntegerField(null=True)
    imageurl = TextField(null=True)
    isallownotifyme = BooleanField(null=True)
    isimmediatedelivery = BooleanField(null=True)
    minimumorderfreedelivery = TextField(null=True)
    minimumspend = BigIntegerField(null=True)
    minimumspendextrafee = BigIntegerField(null=True)
    name = TextField(null=True)
    opensat = TextField(null=True)
    parentbrandid = TextField(null=True)
    pricingtype = TextField(null=True)
    productscount = BigIntegerField(null=True)
    productsimageurl = TextField(null=True)
    promotiontext = TextField(null=True)
    reservedtags = TextField(null=True)
    samestoreprice = TextField(null=True)
    servicetype = TextField(null=True)
    shippingmode = TextField(null=True)
    shippingmodes = TextField(null=True)
    slug = TextField(null=True)
    storeid = BigIntegerField(null=True)
    substituteoptionlist = TextField(null=True)
    tags = TextField(null=True)

    class Meta:
        table_name = 'brands'
        primary_key = False

