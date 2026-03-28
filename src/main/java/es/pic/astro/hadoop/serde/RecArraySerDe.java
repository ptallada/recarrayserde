/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package es.pic.astro.hadoop.serde;

import java.io.IOException;
import java.lang.reflect.Array;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveBaseChar;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

import nom.tam.fits.BinaryTableHDU;
import nom.tam.fits.FitsException;
import nom.tam.fits.FitsFactory;
import nom.tam.util.BufferedDataOutputStream;

/**
 * The following column types are supported: BOOLEAN, TINYINT, SMALLINT, INT,
 * BIGINT, FLOAT, DOUBLE, DECIMAL, DATE, TIMESTAMP, CHAR, VARCHAR and STRING.
 *
 * The rest of column types are not supported: BINARY, ARRAY, MAP, STRUCT and
 * UNION.
 *
 * The mapping between Hive and FITS types is as follows:
 * - BOOLEAN: `L` serialized as `T`, `F` or space
 * - TINYINT: `B` (You need a TZERO card to properly store the values)
 * - SMALLINT: `I`
 * - INT: `J`
 * - BIGINT: `K`
 * - FLOAT: `E`
 * - DOUBLE, DECIMAL: `D`
 * - CHAR, VARCHAR: `nA` (where n is the type length)
 * - STRING: `255A`
 * - DATE: `10A` (ISO-8601: "yyyy-MM-dd")
 * - TIMESTAMP: `23A` (ISO-8601: "yyyy-MM-dd'T'HH:mm:ss.SSS")
 *
 * DECIMAL values are serialized as IEEE 754 double values, so precision/scale
 * are not preserved exactly in all cases.
 *
 * NULL values are serialized following FITS standard:
 * - BOOLEAN: ` `
 * - TINYINT, SMALLINT, INT, BIGINT: `Type.MIN_VALUE`
 * - FLOAT, DOUBLE, DECIMAL: `NaN`
 * - CHAR, VARCHAR, STRING: All bytes are `\0`
 * - DATE, TIMESTAMP: All bytes are `\0`
 */
@SerDeSpec(schemaProps = { serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES })
public class RecArraySerDe extends AbstractSerDe {

  public static final int STRING_FIELD_LENGTH = 255;
  public static final String DATE_FORMAT = "yyyy-MM-dd";
  public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";

  private List<String> columnNames;
  private List<TypeInfo> columnTypes;

  private TypeInfo rowTypeInfo;
  private StructObjectInspector rowObjectInspector;

  /**
   * Mutable per-column buffers used as the serialization source for one row.
   */
  private Object[] row;

  private final BytesWritable serializeBytesWritable = new BytesWritable();
  private final ByteStream.Output output = new ByteStream.Output();
  private final BufferedDataOutputStream buffer = new BufferedDataOutputStream(output);

  private DateFormat df;
  private DateFormat tf;

  /**
   * Per-column fixed lengths for string-backed FITS fields:
   * BOOLEAN -> 1
   * CHAR/VARCHAR -> declared length
   * STRING -> STRING_FIELD_LENGTH
   * DATE -> DATE_FORMAT.length()
   * TIMESTAMP -> TIMESTAMP_FORMAT.length()
   * Non-string-backed columns -> 0
   */
  private int[] columnStringLengths;

  /**
   * Cached null-filled strings per column for string-backed FITS fields.
   */
  private String[] nullStringValues;

  /**
   * Cached struct fields and per-column writers to avoid per-row type switching.
   */
  private StructField[] structFields;
  private ColumnWriter[] writers;

  private interface ColumnWriter {
    void write(Object rowData, StructObjectInspector soi, StructField field, Object dest) throws SerDeException;
  }

  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {

    df = new SimpleDateFormat(DATE_FORMAT);
    tf = new SimpleDateFormat(TIMESTAMP_FORMAT);

    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);

    if (columnNameProperty == null || columnNameProperty.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(","));
    }

    if (columnTypeProperty == null || columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    if (columnNames.size() != columnTypes.size()) {
      throw new SerDeException(
          "Mismatched column counts: names=" + columnNames.size() + ", types=" + columnTypes.size());
    }

    rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    rowObjectInspector = (StructObjectInspector) TypeInfoUtils
        .getStandardWritableObjectInspectorFromTypeInfo(rowTypeInfo);

    row = new Object[columnNames.size()];
    columnStringLengths = new int[columnNames.size()];
    nullStringValues = new String[columnNames.size()];

    for (int i = 0; i < columnNames.size(); i++) {
      TypeInfo type = columnTypes.get(i);

      switch (type.getCategory()) {
      case PRIMITIVE: {
        PrimitiveTypeInfo ptype = (PrimitiveTypeInfo) type;

        switch (ptype.getPrimitiveCategory()) {
        case BOOLEAN:
          columnStringLengths[i] = 1;
          nullStringValues[i] = " ";
          row[i] = new String[] { " " };
          break;

        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
          row[i] = Array.newInstance(ptype.getPrimitiveTypeEntry().primitiveJavaType, 1);
          break;

        case DECIMAL:
          row[i] = new double[1];
          break;

        case CHAR:
        case VARCHAR: {
          int len = ((BaseCharTypeInfo) type).getLength();
          columnStringLengths[i] = len;
          nullStringValues[i] = makeNullString(len);
          row[i] = new String[] { StringUtils.repeat(' ', len) };
          break;
        }

        case STRING:
          columnStringLengths[i] = STRING_FIELD_LENGTH;
          nullStringValues[i] = makeNullString(STRING_FIELD_LENGTH);
          row[i] = new String[] { StringUtils.repeat(' ', STRING_FIELD_LENGTH) };
          break;

        case DATE:
          columnStringLengths[i] = DATE_FORMAT.length();
          nullStringValues[i] = makeNullString(columnStringLengths[i]);
          row[i] = new String[] { df.format(new Date()) };
          break;

        case TIMESTAMP:
          columnStringLengths[i] = TIMESTAMP_FORMAT.length();
          nullStringValues[i] = makeNullString(columnStringLengths[i]);
          row[i] = new String[] { tf.format(new Date()) };
          break;

        case BINARY:
        case VOID:
        default:
          throw new SerDeException("Non supported column primitive type category: " + ptype.getPrimitiveCategory());
        }
        break;
      }

      default:
        throw new SerDeException("Non supported column type category: " + type.getCategory());
      }
    }

    FitsFactory.setUseAsciiTables(false);
    try {
      BinaryTableHDU hdu = (BinaryTableHDU) FitsFactory.hduFactory(row);
      Object[] model = hdu.getData().getModelRow();

      if (model.length != row.length) {
        throw new SerDeException("Unexpected FITS model length: " + model.length + " != " + row.length);
      }
    } catch (FitsException e) {
      throw new SerDeException(e);
    }

    structFields = rowObjectInspector.getAllStructFieldRefs().toArray(new StructField[0]);
    writers = new ColumnWriter[columnNames.size()];
    for (int i = 0; i < columnNames.size(); i++) {
      writers[i] = buildWriter(i, structFields[i].getFieldObjectInspector());
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return BytesWritable.class;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return rowObjectInspector;
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    return row;
  }

  private static String makeNullString(int len) {
    return StringUtils.repeat('\0', len);
  }

  private static String toFitsFixedString(String s, int len) {
    char[] out = new char[len];
    Arrays.fill(out, '\0');

    if (s != null) {
      int n = Math.min(s.length(), len);
      s.getChars(0, n, out, 0);
    }

    return new String(out);
  }

  private ColumnWriter buildWriter(final int columnIndex, final ObjectInspector oi) throws SerDeException {
    switch (oi.getCategory()) {
    case PRIMITIVE: {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;

      switch (poi.getPrimitiveCategory()) {
      case BOOLEAN:
        return buildBooleanWriter((BooleanObjectInspector) poi);
      case BYTE:
        return buildByteWriter((ByteObjectInspector) poi);
      case SHORT:
        return buildShortWriter((ShortObjectInspector) poi);
      case INT:
        return buildIntWriter((IntObjectInspector) poi);
      case LONG:
        return buildLongWriter((LongObjectInspector) poi);
      case FLOAT:
        return buildFloatWriter((FloatObjectInspector) poi);
      case DOUBLE:
        return buildDoubleWriter((DoubleObjectInspector) poi);
      case DECIMAL:
        return buildDecimalWriter((HiveDecimalObjectInspector) poi);
      case CHAR:
      case VARCHAR:
        return buildCharWriter(columnIndex, poi);
      case STRING:
        return buildStringWriter(columnIndex, (StringObjectInspector) poi);
      case DATE:
        return buildDateWriter(columnIndex, (DateObjectInspector) poi);
      case TIMESTAMP:
        return buildTimestampWriter(columnIndex, (TimestampObjectInspector) poi);
      case BINARY:
      case VOID:
      default:
        throw new SerDeException("Non supported column primitive type category: " + poi.getPrimitiveCategory());
      }
    }

    default:
      throw new SerDeException("Non supported column type category: " + oi.getCategory());
    }
  }

  private ColumnWriter buildBooleanWriter(final BooleanObjectInspector poi) {
    return new ColumnWriter() {
      @Override
      public void write(Object rowData, StructObjectInspector soi, StructField field, Object dest) {
        Object data = soi.getStructFieldData(rowData, field);
        String[] out = (String[]) dest;
        out[0] = (data == null) ? " " : (poi.get(data) ? "T" : "F");
      }
    };
  }

  private ColumnWriter buildByteWriter(final ByteObjectInspector poi) {
    return new ColumnWriter() {
      @Override
      public void write(Object rowData, StructObjectInspector soi, StructField field, Object dest) {
        Object data = soi.getStructFieldData(rowData, field);
        ((byte[]) dest)[0] = (data != null) ? poi.get(data) : Byte.MIN_VALUE;
      }
    };
  }

  private ColumnWriter buildShortWriter(final ShortObjectInspector poi) {
    return new ColumnWriter() {
      @Override
      public void write(Object rowData, StructObjectInspector soi, StructField field, Object dest) {
        Object data = soi.getStructFieldData(rowData, field);
        ((short[]) dest)[0] = (data != null) ? poi.get(data) : Short.MIN_VALUE;
      }
    };
  }

  private ColumnWriter buildIntWriter(final IntObjectInspector poi) {
    return new ColumnWriter() {
      @Override
      public void write(Object rowData, StructObjectInspector soi, StructField field, Object dest) {
        Object data = soi.getStructFieldData(rowData, field);
        ((int[]) dest)[0] = (data != null) ? poi.get(data) : Integer.MIN_VALUE;
      }
    };
  }

  private ColumnWriter buildLongWriter(final LongObjectInspector poi) {
    return new ColumnWriter() {
      @Override
      public void write(Object rowData, StructObjectInspector soi, StructField field, Object dest) {
        Object data = soi.getStructFieldData(rowData, field);
        ((long[]) dest)[0] = (data != null) ? poi.get(data) : Long.MIN_VALUE;
      }
    };
  }

  private ColumnWriter buildFloatWriter(final FloatObjectInspector poi) {
    return new ColumnWriter() {
      @Override
      public void write(Object rowData, StructObjectInspector soi, StructField field, Object dest) {
        Object data = soi.getStructFieldData(rowData, field);
        ((float[]) dest)[0] = (data != null) ? poi.get(data) : Float.NaN;
      }
    };
  }

  private ColumnWriter buildDoubleWriter(final DoubleObjectInspector poi) {
    return new ColumnWriter() {
      @Override
      public void write(Object rowData, StructObjectInspector soi, StructField field, Object dest) {
        Object data = soi.getStructFieldData(rowData, field);
        ((double[]) dest)[0] = (data != null) ? poi.get(data) : Double.NaN;
      }
    };
  }

  private ColumnWriter buildDecimalWriter(final HiveDecimalObjectInspector poi) {
    return new ColumnWriter() {
      @Override
      public void write(Object rowData, StructObjectInspector soi, StructField field, Object dest) {
        Object data = soi.getStructFieldData(rowData, field);
        ((double[]) dest)[0] = (data != null) ? poi.getPrimitiveJavaObject(data).doubleValue() : Double.NaN;
      }
    };
  }

  private ColumnWriter buildCharWriter(final int columnIndex, final PrimitiveObjectInspector poi) {
    final int len = columnStringLengths[columnIndex];
    final String nullValue = nullStringValues[columnIndex];

    return new ColumnWriter() {
      @Override
      public void write(Object rowData, StructObjectInspector soi, StructField field, Object dest) {
        Object data = soi.getStructFieldData(rowData, field);
        String[] out = (String[]) dest;

        if (data != null) {
          String s = ((HiveBaseChar) poi.getPrimitiveJavaObject(data)).getValue();
          out[0] = toFitsFixedString(s, len);
        } else {
          out[0] = nullValue;
        }
      }
    };
  }

  private ColumnWriter buildStringWriter(final int columnIndex, final StringObjectInspector poi) {
    final int len = columnStringLengths[columnIndex];
    final String nullValue = nullStringValues[columnIndex];

    return new ColumnWriter() {
      @Override
      public void write(Object rowData, StructObjectInspector soi, StructField field, Object dest) {
        Object data = soi.getStructFieldData(rowData, field);
        String[] out = (String[]) dest;
        out[0] = (data != null) ? toFitsFixedString(poi.getPrimitiveJavaObject(data), len) : nullValue;
      }
    };
  }

  private ColumnWriter buildDateWriter(final int columnIndex, final DateObjectInspector poi) {
    final String nullValue = nullStringValues[columnIndex];

    return new ColumnWriter() {
      @Override
      public void write(Object rowData, StructObjectInspector soi, StructField field, Object dest) {
        Object data = soi.getStructFieldData(rowData, field);
        String[] out = (String[]) dest;

        if (data != null) {
          Date d = new Date(poi.getPrimitiveJavaObject(data).toEpochMilli());
          out[0] = df.format(d);
        } else {
          out[0] = nullValue;
        }
      }
    };
  }

  private ColumnWriter buildTimestampWriter(final int columnIndex, final TimestampObjectInspector poi) {
    final String nullValue = nullStringValues[columnIndex];

    return new ColumnWriter() {
      @Override
      public void write(Object rowData, StructObjectInspector soi, StructField field, Object dest) {
        Object data = soi.getStructFieldData(rowData, field);
        String[] out = (String[]) dest;

        if (data != null) {
          Date d = new Date(poi.getPrimitiveJavaObject(data).toEpochMilli());
          out[0] = tf.format(d);
        } else {
          out[0] = nullValue;
        }
      }
    };
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    StructObjectInspector soi = (StructObjectInspector) objInspector;

    if (structFields.length != writers.length || writers.length != row.length) {
      throw new SerDeException("Internal serializer state is inconsistent");
    }

    output.reset();

    for (int i = 0; i < writers.length; i++) {
      writers[i].write(obj, soi, structFields[i], row[i]);
    }

    try {
      buffer.writeArray(row);
      buffer.flush();
    } catch (IOException e) {
      throw new SerDeException(e);
    }

    serializeBytesWritable.set(output.getData(), 0, output.getLength());
    return serializeBytesWritable;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }
}
