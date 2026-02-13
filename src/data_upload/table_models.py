from sqlalchemy import Table, Column, String, Integer, Text, Date, MetaData, ForeignKey, Numeric

metadata_obj = MetaData()

causes_table = Table(
    'wildfirecause',
    metadata_obj,
    Column('cause_id', Integer, primary_key=True),
    Column('cause_text', Text, nullable=False),
    schema='public'
)

wildfire_table = Table(
    'wildfire',
    metadata_obj,
    Column('wildfire_id', Integer, primary_key=True),
    Column('state_id', String(2), nullable=False),
    Column('fire_name', Text, nullable=False),
    Column('report_date', Date, nullable=False),
    Column('containment_date', Date),
    Column('cause_id', ForeignKey('wildfirecause.cause_id')),
    schema='public'
)

wildfire_location_table = Table(
    'wildfirelocation',
    metadata_obj,
    Column('wildfire_id', ForeignKey('wildfire.wildfire_id'), primary_key=True),
    Column('longitude', Numeric(precision=10, scale=7), nullable=False),
    Column('latitude', Numeric(precision=10, scale=7), nullable=False),
    schema='public'
)

wildfire_size_class_table = Table(
    'wildfiresizeclass',
    metadata_obj,
    Column('size_class', String(1), primary_key=True),
    Column('min_acreage', Numeric(precision=6, scale=2)),
    Column('max_acreage', Numeric(precision=9, scale=2)),
    schema='public'
)

wildfire_size_table = Table(
    'wildfiresize',
    metadata_obj,
    Column('wildfire_id', ForeignKey('wildfire.wildfire_id'), primary_key=True),
    Column('size_class', ForeignKey('wildfiresizeclass.size_class')),
    Column('acreage', Numeric(precision=9, scale=2)),
    schema='public'
)