from pyiceberg.catalog import load_catalog

catalog = load_catalog("local")

catalog.create_table(
    "lakehouse.bronze_weather",
    schema={
        "timestamp": "timestamp",
        "temperature": "double",
    },
)
