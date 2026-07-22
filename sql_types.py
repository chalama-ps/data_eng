"""Mapping of SQL Server data types to pandas dtypes."""

# SQL Server type -> pandas nullable dtype
SQL_TYPE_MAP = {
    # integers (nullable, so NULLs stay as <NA> not float NaN)
    "tinyint": "Int8", "smallint": "Int16", "int": "Int32", "bigint": "Int64",
    # decimals / floats
    "decimal": "float64", "numeric": "float64", "float": "float64",
    "real": "float32", "money": "float64", "smallmoney": "float64",
    # boolean
    "bit": "boolean",
    # strings
    "char": "string", "varchar": "string", "nchar": "string",
    "nvarchar": "string", "text": "string", "ntext": "string",
    "uniqueidentifier": "string",
    # date/time (parsed to datetime, serialised to ISO strings later)
    "date": "datetime64[ns]", "datetime": "datetime64[ns]",
    "datetime2": "datetime64[ns]", "smalldatetime": "datetime64[ns]",
    "datetimeoffset": "datetime64[ns]",
}
