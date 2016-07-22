RecArraySerDe is a Hive custom SerDe that can be used to write data as a
sequence of NumPy record arrays which, in turn, are the foundation of the
FITS format.

Using this SerDe and generating the FITS headers from the metadata, a valid
FITS file can be constructed by appending the serialized data to the
respective headers and padding the result to 2880-byte boundaries.

The following column types are supported: BOOLEAN, TINYINT, SMALLINT, INT,
BIGINT, FLOAT, DOUBLE, DATE, TIMESTAMP, CHAR, VARCHAR and STRING.

The rest of column types are not supported: DECIMAL, BINARY, ARRAY, MAP,
STRUCT and UNION. 

The mapping between Hive and FITS types is as follows:
 - BOOLEAN:     `L`
 - TINYINT:     `B` (You need a TZERO card to properly store the values)
 - SMALLINT:    `I`
 - INT:         `J`
 - BIGINT:      `K`
 - CHAR,
   VARCHAR,
   STRING:   `255A` (Per-column values after HIVE-13064 fix)
 - DATE:      `10A` (ISO-8601: "yyyy-MM-dd")
 - TIMESTAMP: `23A` (ISO-8601: "yyyy-MM-dd'T'HH:mm:ss.SSS")

NULL values are serialized following FITS standard:
 - BOOLEAN: `\0`
 - TINYINT, SMALLINT, INT, BIGINT: `Type.MIN_VALUE`
 - FLOAT, DOUBLE: `NaN`
 - CHAR, VARCHAR, STRING: All bytes are `\0` (FITS only requires the first)
 - DATE, TIMESTAMP: All bytes are `\0`
