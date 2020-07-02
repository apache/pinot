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
import java.io.FileOutputStream;
import java.net.URL;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PluginManagerTest {

  private final String TEST_RECORD_READER_FILE = "TestRecordReader.java";

  private File tempDir;
  private String jarFile;
  private File jarDirFile;

  @BeforeClass
  public void setup() {

    tempDir = new File(System.getProperty("java.io.tmpdir"), "pinot-plugin-test");
    tempDir.delete();
    tempDir.mkdirs();

    String jarDir = tempDir + "/" + "test-record-reader";
    jarFile = jarDir + "/" + "test-record-reader.jar";
    jarDirFile = new File(jarDir);
    jarDirFile.mkdirs();
  }

  @Test
  public void testSimple()
      throws Exception {
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    URL javaFile = Thread.currentThread().getContextClassLoader().getResource(TEST_RECORD_READER_FILE);
    if (javaFile != null) {
      int compileStatus = compiler.run(null, null, null, javaFile.getFile(), "-d", tempDir.getAbsolutePath());
      Assert.assertTrue(compileStatus == 0, "Error when compiling resource: " + TEST_RECORD_READER_FILE);

      URL classFile = Thread.currentThread().getContextClassLoader().getResource("TestRecordReader.class");

      if (classFile != null) {
        JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarFile));
        jos.putNextEntry(new JarEntry(new File(classFile.getFile()).getName()));
        jos.write(FileUtils.readFileToByteArray(new File(classFile.getFile())));
        jos.closeEntry();
        jos.close();

        PluginManager.get().load("test-record-reader", jarDirFile);

        RecordReader testRecordReader = PluginManager.get().createInstance("test-record-reader", "TestRecordReader");
        testRecordReader.init(null, null, null);
        int count = 0;
        while (testRecordReader.hasNext()) {
          GenericRow row = testRecordReader.next();
          count++;
        }

        Assert.assertEquals(count, 10);
      }
    }
  }

  @Test
  public void testBackwardCompatible() {
    Assert.assertEquals(PluginManager
            .loadClassWithBackwardCompatibleCheck("org.apache.pinot.core.realtime.stream.SimpleAvroMessageDecoder"),
        "org.apache.pinot.plugin.inputformat.avro.SimpleAvroMessageDecoder");
    Assert.assertEquals(PluginManager
            .loadClassWithBackwardCompatibleCheck("org.apache.pinot.core.realtime.stream.SimpleAvroMessageDecoder"),
        "org.apache.pinot.plugin.inputformat.avro.SimpleAvroMessageDecoder");
    Assert.assertEquals(PluginManager
            .loadClassWithBackwardCompatibleCheck("org.apache.pinot.core.realtime.impl.kafka.KafkaAvroMessageDecoder"),
        "org.apache.pinot.plugin.inputformat.avro.KafkaAvroMessageDecoder");
    Assert.assertEquals(PluginManager
            .loadClassWithBackwardCompatibleCheck("org.apache.pinot.core.realtime.impl.kafka.KafkaJSONMessageDecoder"),
        "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder");

    // RecordReader
    Assert.assertEquals(
        PluginManager.loadClassWithBackwardCompatibleCheck("org.apache.pinot.core.data.readers.AvroRecordReader"),
        "org.apache.pinot.plugin.inputformat.avro.AvroRecordReader");
    Assert.assertEquals(
        PluginManager.loadClassWithBackwardCompatibleCheck("org.apache.pinot.core.data.readers.CSVRecordReader"),
        "org.apache.pinot.plugin.inputformat.csv.CSVRecordReader");
    Assert.assertEquals(
        PluginManager.loadClassWithBackwardCompatibleCheck("org.apache.pinot.core.data.readers.JSONRecordReader"),
        "org.apache.pinot.plugin.inputformat.json.JSONRecordReader");
    Assert.assertEquals(
        PluginManager.loadClassWithBackwardCompatibleCheck("org.apache.pinot.orc.data.readers.ORCRecordReader"),
        "org.apache.pinot.plugin.inputformat.orc.ORCRecordReader");
    Assert.assertEquals(
        PluginManager.loadClassWithBackwardCompatibleCheck("org.apache.pinot.parquet.data.readers.ParquetRecordReader"),
        "org.apache.pinot.plugin.inputformat.parquet.ParquetRecordReader");
    Assert.assertEquals(
        PluginManager.loadClassWithBackwardCompatibleCheck("org.apache.pinot.core.data.readers.ThriftRecordReader"),
        "org.apache.pinot.plugin.inputformat.thrift.ThriftRecordReader");

    // PinotFS
    Assert.assertEquals(PluginManager.loadClassWithBackwardCompatibleCheck("org.apache.pinot.filesystem.AzurePinotFS"),
        "org.apache.pinot.plugin.filesystem.AzurePinotFS");
    Assert.assertEquals(PluginManager.loadClassWithBackwardCompatibleCheck("org.apache.pinot.filesystem.HadoopPinotFS"),
        "org.apache.pinot.plugin.filesystem.HadoopPinotFS");
    Assert.assertEquals(PluginManager.loadClassWithBackwardCompatibleCheck("org.apache.pinot.filesystem.LocalPinotFS"),
        "org.apache.pinot.spi.filesystem.LocalPinotFS");

    // StreamConsumerFactory
    Assert.assertEquals(PluginManager
            .loadClassWithBackwardCompatibleCheck("org.apache.pinot.core.realtime.impl.kafka.KafkaConsumerFactory"),
        "org.apache.pinot.plugin.stream.kafka09.KafkaConsumerFactory");
    Assert.assertEquals(PluginManager
            .loadClassWithBackwardCompatibleCheck("org.apache.pinot.core.realtime.impl.kafka2.KafkaConsumerFactory"),
        "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory");
  }

  @AfterClass
  public void tearDown() {
    tempDir.delete();
    FileUtils.deleteQuietly(jarDirFile);
  }
}
