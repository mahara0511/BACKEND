import lancedb
import pyarrow as pa

db = lancedb.connect('database.lance1')
schema = pa.schema([
    pa.field('id',pa.string())
])
db.create_table('USER' , schema=schema)