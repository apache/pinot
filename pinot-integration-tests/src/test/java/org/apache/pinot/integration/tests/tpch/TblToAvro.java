/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.integration.tests.tpch;

import com.google.common.collect.ImmutableMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;


/**
 * A helper class to convert TPC-H .tbl files to Avro format.
 */
public final class TblToAvro {
  private static final String LONG_TYPE = "long";
  private static final String DOUBLE_TYPE = "double";
  private static final String STRING_TYPE = "string";
  private static final String FOLDER_PATH = "/Users/haitaozhang/Downloads/TPC-H-V3.0.1/dbgen/";
  private static final String TBL_FILE_SUFFIX = ".tbl";
  private static final Map<String, String[]> TABLE_NAME_FIELDS_MAP =
      ImmutableMap.of(
          // One example record:
          // {"c_custkey":{"long":1},"c_name":{"string":"Customer#000000001"},
          // "c_address":{"string":"IVhzIApeRb ot,c,E"},"c_nationkey":{"long":15},
          // "c_phone":{"string":"25-989-741-2988"},"c_acctbal":{"double":711.56},"c_mktsegment":{"string":"BUILDING"},
          // "c_comment":{"string":"to the even, regular platelets. regular, ironic epitaphs nag e"}}
          "customer",
          new String[]{
              "c_custkey", LONG_TYPE,
              "c_name", STRING_TYPE,
              "c_address", STRING_TYPE,
              "c_nationkey", LONG_TYPE,
              "c_phone", STRING_TYPE,
              "c_acctbal", DOUBLE_TYPE,
              "c_mktsegment", STRING_TYPE,
              "c_comment", STRING_TYPE},
          // One example record:
          // {"l_orderkey":{"long":600000},"l_partkey":{"long":12916},"l_suppkey":{"long":917},
          // "l_linenumber":{"long":2},"l_quantity":{"long":1},"l_extendedprice":{"double":1828.91},
          // "l_discount":{"double":0.03},"l_tax":{"double":0.0},"l_returnflag":{"string":"N"},
          // "l_linestatus":{"string":"O"},"l_shipdate":{"string":"1998-04-13"},"l_commitdate":{"string":"1998-05-24"},
          // "l_receiptdate":{"string":"1998-04-30"},"l_shipinstruct":{"string":"DELIVER IN PERSON"},
          // "l_shipmode":{"string":"RAIL"},"l_comment":{"string":" wake braids. "}}
          "lineitem",
          new String[]{
              "l_orderkey", LONG_TYPE,
              "l_partkey", LONG_TYPE,
              "l_suppkey", LONG_TYPE,
              "l_linenumber", LONG_TYPE,
              "l_quantity", LONG_TYPE,
              "l_extendedprice", DOUBLE_TYPE,
              "l_discount", DOUBLE_TYPE,
              "l_tax", DOUBLE_TYPE,
              "l_returnflag", STRING_TYPE,
              "l_linestatus", STRING_TYPE,
              "l_shipdate", STRING_TYPE,
              "l_commitdate", STRING_TYPE,
              "l_receiptdate", STRING_TYPE,
              "l_shipinstruct", STRING_TYPE,
              "l_shipmode", STRING_TYPE,
              "l_comment", STRING_TYPE},
          // One example record:
          // {"n_nationkey":{"long":0},"n_name":{"string":"ALGERIA"},"n_regionkey":{"long":0},
          // "n_comment":{"string":" haggle. carefully final deposits detect slyly agai"}}
          "nation",
          new String[]{
              "n_nationkey", LONG_TYPE,
              "n_name", STRING_TYPE,
              "n_regionkey", LONG_TYPE,
              "n_comment", STRING_TYPE},
          // One example record:
          // {"o_orderkey":{"long":1},"o_custkey":{"long":370},"o_orderstatus":{"string":"O"},
          // "o_totalprice":{"double":173665.47},"o_orderdate":{"string":"1996-01-02"},
          // "o_orderpriority":{"string":"5-LOW"},"o_clerk":{"string":"Clerk#000000951"},"o_shippriority":{"long":0},
          // "o_comment":{"string":"ly final dependencies: slyly bold "}}
          "orders",
          new String[]{
              "o_orderkey", LONG_TYPE,
              "o_custkey", LONG_TYPE,
              "o_orderstatus", STRING_TYPE,
              "o_totalprice", DOUBLE_TYPE,
              "o_orderdate", STRING_TYPE,
              "o_orderpriority", STRING_TYPE,
              "o_clerk", STRING_TYPE,
              "o_shippriority", LONG_TYPE,
              "o_comment", STRING_TYPE},
          // One example record:
          // {"p_partkey":{"long":1},"p_name":{"string":"goldenrod lavender spring chocolate lace"},
          // "p_mfgr":{"string":"Manufacturer#1"},"p_brand":{"string":"Brand#13"},
          // "p_type":{"string":"PROMO BRUSHED STEEL"},"p_size":{"long":7},"p_container":{"string":"JUMBO PKG"},
          // "p_retailprice":{"double":901.0},"p_comment":{"string":"ly final dependencies: slyly bold "}}
          "part",
          new String[]{
              "p_partkey", LONG_TYPE,
              "p_name", STRING_TYPE,
              "p_mfgr", STRING_TYPE,
              "p_brand", STRING_TYPE,
              "p_type", STRING_TYPE,
              "p_size", LONG_TYPE,
              "p_container", STRING_TYPE,
              "p_retailprice", DOUBLE_TYPE,
              "p_comment", STRING_TYPE},
          // One example record:
          // {"ps_partkey":{"long":1},"ps_suppkey":{"long":2},"ps_availqty":{"long":997},
          // "ps_supplycost":{"double":6.02},"ps_comment":{"string":"ly final dependencies: slyly bold "}}
          "partsupp",
          new String[]{
              "ps_partkey", LONG_TYPE,
              "ps_suppkey", LONG_TYPE,
              "ps_availqty", LONG_TYPE,
              "ps_supplycost", DOUBLE_TYPE,
              "ps_comment", STRING_TYPE},
          // One example record:
          // {"r_regionkey":{"long":0},"r_name":{"string":"AFRICA"},"r_comment":{"string":"lar deposits. blithe"}}
          "region",
          new String[]{
              "r_regionkey", LONG_TYPE,
              "r_name", STRING_TYPE,
              "r_comment", STRING_TYPE},
          // One example record:
          // {"s_suppkey":{"long":1},"s_name":{"string":"Supplier#000000001"},
          // "s_address":{"string":" N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ"},"s_nationkey":{"long":15},
          // "s_phone":{"string":"25-989-741-2988"},"s_acctbal":{"double":711.56},
          // "s_comment":{"string":" deposits eat slyly ironic, even instructions. express foxes detect slyly.
          // blithely even accounts abov"}}
          "supplier",
          new String[]{
              "s_suppkey", LONG_TYPE,
              "s_name", STRING_TYPE,
              "s_address", STRING_TYPE,
              "s_nationkey", LONG_TYPE,
              "s_phone", STRING_TYPE,
              "s_acctbal", DOUBLE_TYPE,
              "s_comment", STRING_TYPE});

  private TblToAvro() {
  }

  public static void main(String[] args) throws IOException {
//    String tpchTblFileFolder = args[0];
    String tpchTblFileFolder = FOLDER_PATH;
    for (Map.Entry<String, String[]> entry: TABLE_NAME_FIELDS_MAP.entrySet()) {
      String tableName = entry.getKey();
      String[] fieldsAndTypes = entry.getValue();
      SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record(tableName).fields();
      for (int i = 0; i < fieldsAndTypes.length; i += 2) {
        String fieldName = fieldsAndTypes[i];
        String fieldType = fieldsAndTypes[i + 1];
        switch (fieldType) {
          case LONG_TYPE:
            schemaFields.name(fieldName).type().unionOf().longType().and().nullType().endUnion().noDefault();
            break;
          case DOUBLE_TYPE:
            schemaFields.name(fieldName).type().unionOf().doubleType().and().nullType().endUnion().noDefault();
            break;
          case STRING_TYPE:
            schemaFields.name(fieldName).type().unionOf().stringType().and().nullType().endUnion().noDefault();
            break;
          default:
            throw new IllegalStateException("Unsupported field type: " + fieldType);
        }
      }
      Schema schema = schemaFields.endRecord();

      // Open Avro data file for writing
      DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
      DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
      Path avroFilePath = Paths.get(tpchTblFileFolder + tableName + Constants.AVRO_FILE_SUFFIX);
      Files.delete(avroFilePath);
      OutputStream outputStream = Files.newOutputStream(avroFilePath);
      dataFileWriter.create(schema, outputStream);

      // Read TPC-H .tbl files and convert to Avro format
      try (BufferedReader reader = new BufferedReader(
          new FileReader(tpchTblFileFolder + tableName + TBL_FILE_SUFFIX))) {
        String line;
        while ((line = reader.readLine()) != null) {
          String[] fields = line.split("\\|");
          GenericRecord record = new GenericData.Record(schema);
          for (int i = 0; i < fields.length; i++) {
            String fieldName = fieldsAndTypes[2 * i];
            String fieldType = fieldsAndTypes[2 * i + 1];
            switch (fieldType) {
              case LONG_TYPE:
                record.put(fieldName, Long.parseLong(fields[i]));
                break;
              case DOUBLE_TYPE:
                record.put(fieldName, Double.parseDouble(fields[i]));
                break;
              case STRING_TYPE:
                record.put(fieldName, fields[i]);
                break;
              default:
                throw new IllegalStateException("Unsupported field type: " + fieldType);
            }
          }
          dataFileWriter.append(record);
        }
      }

      // Close Avro data file
      dataFileWriter.close();
    }
  }
}
