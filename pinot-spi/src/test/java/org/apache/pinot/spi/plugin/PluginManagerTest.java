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
package org.apache.pinot.spi.plugin;

import java.io.File;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
//import org.xeustechnologies.jcl.JarClassLoader;
//import org.xeustechnologies.jcl.JclObjectFactory;


public class PluginManagerTest {
//  public static void main(String[] args)
//      throws Exception {
////    testAvro();
//
////    testParquetWithJcl();
//    testParquet();
//  }
//
//  private static void testParquetWithJcl()
//      throws Exception {
//    File dir = new File("/tmp/pinot-plugins/pinot-parquet");
//    PluginManager pluginManager = PluginManager.get();
//    String pluginName = "pinot-parquet";
//    pluginManager.load(pluginName, dir);
//
//    JarClassLoader jcl = new JarClassLoader();
//    jcl.add(dir.toURI().toURL());
//
//    String parquetFilePath = "/tmp/sample.c000.snappy.parquet";
//    File pinotSchemaFile = new File("/tmp/ops_wb_table_schema.json");
//    Schema pinotSchema = Schema.fromFile(pinotSchemaFile);
//    String recordReaderClassName = "org.apache.pinot.parquet.data.readers.ParquetRecordReader";
//
//    jcl.loadClass(recordReaderClassName, true);
//    JclObjectFactory objectFactory = JclObjectFactory.getInstance();
//    RecordReader recordReader = (RecordReader) objectFactory.create(jcl, recordReaderClassName);
//    recordReader.init(new File(parquetFilePath), pinotSchema, null);
//    while (recordReader.hasNext()) {
//      GenericRow row = recordReader.next();
//      System.out.println("row = " + row);
//    }
//  }
//
//  private static void testParquet()
//      throws Exception {
//    File dir = new File("/tmp/pinot-plugins/pinot-parquet");
//    PluginManager pluginManager = PluginManager.get();
//    String pluginName = "pinot-parquet";
//    pluginManager.load(pluginName, dir);
//
//    String recordReaderClassName = "org.apache.pinot.parquet.data.readers.ParquetRecordReader";
//    Class<?> recordReaderClass = pluginManager.loadClass(pluginName, recordReaderClassName);
//    Class<?> fsClass = pluginManager.loadClass(pluginName, "org.apache.hadoop.fs.LocalFileSystem");
//    System.out.println("recordReaderClass = " + recordReaderClass);
//
//    String parquetFilePath = "/tmp/sample.c000.snappy.parquet";
//    File pinotSchemaFile = new File("/tmp/ops_wb_table_schema.json");
//    Schema pinotSchema = Schema.fromFile(pinotSchemaFile);
//
//    RecordReader recordReader =
//        pluginManager.createInstance(pluginName, recordReaderClassName, new Class[]{}, new Object[]{});
//    Thread.currentThread().setContextClassLoader(recordReader.getClass().getClassLoader());
//    recordReader.init(new File(parquetFilePath), pinotSchema, null);
//
//    while (recordReader.hasNext()) {
//      GenericRow row = recordReader.next();
//      System.out.println("row = " + row);
//    }
//  }
//
//  private static void testAvro()
//      throws ClassNotFoundException {
//    File dir =
//        new File("/Users/kishoreg/projects/incubator-pinot/pinot-record-readers/pinot-avro/target/pinot-avro-pkg");
//    PluginManager pluginManager = PluginManager.get();
//    pluginManager.load("pinot-avro", dir);
//
//    Class<?> recordReaderClass =
//        pluginManager.loadClass("pinot-avro", "org.apache.pinot.avro.data.readers.AvroRecordReader");
//    System.out.println("recordReaderClass = " + recordReaderClass);
//
//    String avroFilePath =
//        "/Users/kishoreg/projects/incubator-pinot/pinot-tools/src/main/resources/sample_data/airlineStats_data.avro";
//  }
}
