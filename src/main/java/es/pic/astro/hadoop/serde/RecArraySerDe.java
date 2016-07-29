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
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.ByteStream.RandomAccessOutput;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveIntervalDayTimeObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveIntervalYearMonthObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import nom.tam.fits.BinaryTableHDU;
import nom.tam.fits.FitsException;
import nom.tam.fits.FitsFactory;
import nom.tam.util.ArrayFuncs;
import nom.tam.util.BufferedDataOutputStream;

/**
 * RecArraySerDe can be used to write data as a sequence of **NumPy** record arrays
 * which, in turn, are the foundation of the **FITS** format.
 * 
 * Using this SerDe and generating the FITS headers from the metadata, a valid
 * FITS file can be constructed by appending the serialized data to the
 * respective headers and padding the result to 2880-byte boundaries.
 * 
 * The following column types are supported: BOOLEAN, TINYINT, SMALLINT, INT,
 * BIGINT, FLOAT, DOUBLE, DATE, TIMESTAMP, CHAR, VARCHAR and STRING.
 * 
 * The rest of column types are not supported: DECIMAL, BINARY, ARRAY, MAP,
 * STRUCT and UNION. 
 * 
 * The mapping between Hive and FITS types is as follows:
 *  - BOOLEAN:     `L`
 *  - TINYINT:     `B` (You need a TZERO card to properly store the values)
 *  - SMALLINT:    `I`
 *  - INT:         `J`
 *  - BIGINT:      `K`
 *  - CHAR,
 *    VARCHAR,
 *    STRING:   `255A` (Per-column values after HIVE-13064 fix)
 *  - DATE:      `10A` (ISO-8601: "yyyy-MM-dd")
 *  - TIMESTAMP: `23A` (ISO-8601: "yyyy-MM-dd'T'HH:mm:ss.SSS")
 * 
 * NULL values are serialized following FITS standard:
 *  - BOOLEAN: `\0`
 *  - TINYINT, SMALLINT, INT, BIGINT: `Type.MIN_VALUE`
 *  - FLOAT, DOUBLE: `NaN`
 *  - CHAR, VARCHAR, STRING: All bytes are `\0` (FITS only requires the first)
 *  - DATE, TIMESTAMP: All bytes are `\0`
 */
@SerDeSpec(
  schemaProps = {
    serdeConstants.LIST_COLUMNS, 
    serdeConstants.LIST_COLUMN_TYPES
  }
)

public class RecArraySerDe extends AbstractSerDe {

  public static final int STRING_FIELD_LENGTH = 255;
  public static final String DATE_FORMAT = "yyyy-MM-dd";
  public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";
  
  List<String> columnNames;
  List<TypeInfo> columnTypes;

  TypeInfo rowTypeInfo;
  StructObjectInspector rowObjectInspector;

  Object[] row;
  Object[] model;

  BinaryTableHDU hdu;
  
  BytesWritable serializeBytesWritable = new BytesWritable();
  ByteStream.Output output = new ByteStream.Output();
  BufferedDataOutputStream buffer = new BufferedDataOutputStream(output);
  
  DateFormat df;
  DateFormat tf;
  
  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {

    df = new SimpleDateFormat(DATE_FORMAT);
    tf = new SimpleDateFormat(TIMESTAMP_FORMAT);
    
    // Get column names and types
    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    if (columnNameProperty.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(","));
    }
    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }
    assert (columnNames.size() == columnTypes.size());
    
    // Create row related objects
    rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    rowObjectInspector = (StructObjectInspector) TypeInfoUtils
        .getStandardWritableObjectInspectorFromTypeInfo(rowTypeInfo);
    
    // Create sample row
    row = new Object[columnNames.size()];
    for (int i = 0; i < columnNames.size(); i++) {
      TypeInfo type = columnTypes.get(i);
      switch (type.getCategory()) {
        case PRIMITIVE: {
          PrimitiveTypeInfo ptype = (PrimitiveTypeInfo) type;
          switch (ptype.getPrimitiveCategory()) {
            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
              row[i] = Array.newInstance(ptype.getPrimitiveTypeEntry().primitiveJavaType, 1);
              break;
            case CHAR:
            case VARCHAR:
            case STRING:
              row[i] = new String[]{StringUtils.repeat(' ', STRING_FIELD_LENGTH)};
              break;
            case DATE:
              row[i] = new String[]{df.format(new Date())};
              break;
            case TIMESTAMP:
              row[i] = new String[]{tf.format(new Date())};
              break;
            case BINARY:
            case DECIMAL:
            case VOID:
            default: {
              throw new SerDeException("Non supported column primitive type category: " + ptype.getPrimitiveCategory());
            }
          }
          break;
        }
        default: {
          throw new SerDeException("Non supported column type category: "+ type.getCategory());
        }
      }
    }
    
    // Build empty model row from sample
    FitsFactory.setUseAsciiTables(false);
    try {
      hdu = (BinaryTableHDU) FitsFactory.HDUFactory(row);
    } catch (FitsException e) {
      throw new SerDeException(e);
    }
    model = hdu.getData().getModelRow();
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

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    
    StructField field;
    Object data;
    ObjectInspector oi;
    
    output.reset();
    
    for (int i = 0; i < columnNames.size(); i++) {
      field = fields.get(i);
      data = soi.getStructFieldData(obj, field);
      oi = field.getFieldObjectInspector();
      
      switch (oi.getCategory()) {
        case PRIMITIVE: {
          PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
          switch (poi.getPrimitiveCategory()) {
            case BOOLEAN:
              if (data != null) {
                if (((BooleanObjectInspector) poi).get(data)) {
                  model[i] = new String[]{"T"};
                } else {
                  model[i] = new String[]{"F"};
                }
              }
              break;
            case BYTE:
              if (data != null) {
                model[i] = new byte[]{((ByteObjectInspector) poi).get(data)};
              } else {
                model[i] = new byte[]{Byte.MIN_VALUE};
              }
              break;
            case SHORT:
              if (data != null) {
                model[i] = new short[]{((ShortObjectInspector) poi).get(data)};
              } else {
                model[i] = new short[]{Short.MIN_VALUE};
              }
              break;
            case INT:
              if (data != null) {
                model[i] = new int[]{((IntObjectInspector) poi).get(data)};
              } else {
                model[i] = new int[]{Integer.MIN_VALUE};
              }
              break;
            case LONG:
              if (data != null) {
                model[i] = new long[]{((LongObjectInspector) poi).get(data)};
              } else {
                model[i] = new long[]{Long.MIN_VALUE};
              }
              break;
            case FLOAT:
              if (data != null) {
                model[i] = new float[]{((FloatObjectInspector) poi).get(data)};
              } else {
                model[i] = new float[]{Float.NaN};
              }
              break;
            case DOUBLE:
              if (data != null) {
                model[i] = new double[]{((DoubleObjectInspector) poi).get(data)};
              } else {
                model[i] = new double[]{Double.NaN};
              }
              break;
            case CHAR:
            case VARCHAR:
            case STRING:
              if (data != null) {
                // TODO: Get the length of the model row when HIVE-13064 is
                // fixed to implement specific per-column lengths.
                int len = STRING_FIELD_LENGTH;
                String s = ((StringObjectInspector) poi).getPrimitiveJavaObject(data);
                model[i] = new String[]{StringUtils.rightPad(s, len, '\0').substring(0, len)};
              }
              break;
            case DATE:
              if (data != null) {
                Date d = ((DateObjectInspector) poi).getPrimitiveJavaObject(data);
                model[i] = new String[]{df.format(d)};
              }
              break;
            case TIMESTAMP:
              if (data != null) {
                Date d = new Date(((TimestampObjectInspector) poi).getPrimitiveJavaObject(data).getTime());
                model[i] = new String[]{tf.format(d)};
              }
              break;
            case BINARY:
            case DECIMAL:
            case VOID:
            default: {
              throw new SerDeException("Non supported column primitive type category: " + poi.getPrimitiveCategory());
            }
          }
          break;
        }
        default: {
          throw new SerDeException("Non supported column type category: "+ oi.getCategory());
        }
      }
    }
    
    try{
      buffer.writeArray(model);
      buffer.flush();
    } catch (IOException e) {
      throw new SerDeException(e);
    }
    
    serializeBytesWritable.set(output.getData(), 0, output.getLength());
    
    return serializeBytesWritable;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }
}
