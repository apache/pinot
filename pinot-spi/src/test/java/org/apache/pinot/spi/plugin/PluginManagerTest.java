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
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
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

  private static final String TEST_RECORD_READER_FILE = "TestRecordReader.java";

  private File _tempDir;
  private String _jarFile;
  private File _jarDirFile;

  private File _p1;
  private File _p1Copy;
  private File _p2;
  private File _p3;
  private File _p4;

  @BeforeClass
  public void setup()
      throws IOException {
    _tempDir = new File(System.getProperty("java.io.tmpdir"), "pinot-plugin-test");
    FileUtils.deleteDirectory(_tempDir);
    _tempDir.mkdirs();
  }

  @Test
  public void testGetPluginsToLoad()
      throws IOException {
    /* We have two plugin directories (../plugins/d1/ and ../plugins/d2/)
     * plugins to include = [ p1, p2, p3 ]
     * d1 has plugins: p1
     * d2 has plugins: p1, p2, p3, p4
     * We expect d1/p1, d2/p2, d2/p3 to be picked up
     *   - ensuring second instance of p1 is ignored
     *   - ensuring p4 is ignored as it's not on the plugins to include list
     */

    String pluginsDirs = _tempDir + "/plugins/d1;" + _tempDir + "/plugins/d2;";
    String pluginsToInclude = "p1;p2;p3"; // specifically excluding p3.jar

    File pluginsDir = new File(_tempDir + "/plugins");
    pluginsDir.mkdir();
    File subPluginsDir1 = new File(pluginsDir + "/d1");
    subPluginsDir1.mkdir();
    File subPluginsDir2 = new File(pluginsDir + "/d2");
    subPluginsDir2.mkdir();

    _p1 = new File(pluginsDir + "/d1/p1/p1.jar");
    FileUtils.touch(_p1);
    _p1Copy = new File(pluginsDir + "/d2/p1/p1.jar");
    FileUtils.touch(_p1Copy);
    _p2 = new File(pluginsDir + "/d2/p2/p2.jar");
    FileUtils.touch(_p2);
    _p3 = new File(pluginsDir + "/d2/p3/p3.jar");
    FileUtils.touch(_p3);
    _p4 = new File(pluginsDir + "/d2/p4/p4.jar");
    FileUtils.touch(_p4);

    HashMap<String, File> actualPluginsMap = PluginManager.get().getPluginsToLoad(pluginsDirs, pluginsToInclude);
    Assert.assertEquals(actualPluginsMap.size(), 3);

    HashMap<String, String> actualPluginNamesAndPaths = new HashMap<>();
    for (Map.Entry<String, File> entry : actualPluginsMap.entrySet()) {
      actualPluginNamesAndPaths.put(entry.getKey(), entry.getValue().getAbsolutePath());
    }
    HashMap<String, String> expectedPluginNamesAndPaths = new HashMap<>();
    expectedPluginNamesAndPaths.put("p1", _p1.getParentFile().getAbsolutePath());
    expectedPluginNamesAndPaths.put("p2", _p2.getParentFile().getAbsolutePath());
    expectedPluginNamesAndPaths.put("p3", _p3.getParentFile().getAbsolutePath());

    Assert.assertEquals(actualPluginNamesAndPaths, expectedPluginNamesAndPaths);
  }

  @Test
  public void testSimple()
      throws Exception {

    String jarDir = _tempDir + "/test-record-reader";
    _jarFile = jarDir + "/test-record-reader.jar";
    _jarDirFile = new File(jarDir);
    _jarDirFile.mkdirs();

    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    URL javaFile = Thread.currentThread().getContextClassLoader().getResource(TEST_RECORD_READER_FILE);
    if (javaFile != null) {
      int compileStatus = compiler.run(null, null, null, javaFile.getFile(), "-d", _tempDir.getAbsolutePath());
      Assert.assertTrue(compileStatus == 0, "Error when compiling resource: " + TEST_RECORD_READER_FILE);

      URL classFile = Thread.currentThread().getContextClassLoader().getResource("TestRecordReader.class");

      if (classFile != null) {
        JarOutputStream jos = new JarOutputStream(new FileOutputStream(_jarFile));
        jos.putNextEntry(new JarEntry(new File(classFile.getFile()).getName()));
        jos.write(FileUtils.readFileToByteArray(new File(classFile.getFile())));
        jos.closeEntry();
        jos.close();

        PluginManager.get().load("test-record-reader", _jarDirFile);

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
            .loadClassWithBackwardCompatibleCheck("org.apache.pinot.core.realtime.impl.kafka2.KafkaConsumerFactory"),
        "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory");
  }

  @AfterClass
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(_tempDir);
    FileUtils.deleteQuietly(_jarDirFile);
  }
}
