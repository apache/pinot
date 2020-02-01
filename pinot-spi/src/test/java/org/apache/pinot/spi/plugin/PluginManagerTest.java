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
    URL javaFile = Thread.currentThread().getContextClassLoader().getResource("TestRecordReader.java");
    if (javaFile != null) {
      compiler.run(null, null, null, javaFile.getFile(), "-d", tempDir.getAbsolutePath());

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
    Assert.assertEquals(PluginManager.loadClassWithBackwardCompatibleCheck("oldTestRecordReader"), "oldTestRecordReader");
    PluginManager.PLUGINS_BACKWARD_COMPATIBLE_CLASS_NAME_MAP.put("oldTestRecordReader","TestRecordReader");
    Assert.assertEquals(PluginManager.loadClassWithBackwardCompatibleCheck("oldTestRecordReader"), "TestRecordReader");
  }

  @AfterClass
  public void tearDown() {
    tempDir.delete();
    FileUtils.deleteQuietly(jarDirFile);
  }
}
