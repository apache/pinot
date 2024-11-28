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
package org.apache.pinot.controller.api;

import java.io.File;
import java.net.URI;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.resources.ControllerFilePathProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class ControllerFilePathProviderTest {
  private static final String HOST = "localhost";
  private static final String PORT = "12345";
  private static final File DATA_DIR = new File(FileUtils.getTempDirectory(), "ControllerFilePathProviderTest");
  private static final File LOCAL_TEMP_DIR = new File(DATA_DIR, "localTemp");

  @Test
  public void testLocalTempDirConfigured()
      throws Exception {
    FileUtils.deleteQuietly(DATA_DIR);
    PinotFSFactory.init(new PinotConfiguration());

    ControllerConf controllerConf = new ControllerConf();
    controllerConf.setControllerHost(HOST);
    controllerConf.setControllerPort(PORT);
    controllerConf.setDataDir(DATA_DIR.getPath());
    controllerConf.setLocalTempDir(LOCAL_TEMP_DIR.getPath());
    ControllerFilePathProvider.init(controllerConf);
    ControllerFilePathProvider provider = ControllerFilePathProvider.getInstance();

    URI dataDirURI = provider.getDataDirURI();
    assertEquals(dataDirURI, new URI("file:" + DATA_DIR.getAbsolutePath()));
    assertTrue(new File(dataDirURI).isDirectory());

    File fileUploadTempDir = provider.getFileUploadTempDir();
    assertEquals(fileUploadTempDir, new File(LOCAL_TEMP_DIR, "fileUploadTemp"));
    checkDirExistAndEmpty(fileUploadTempDir);

    File untarredFileTempDir = provider.getUntarredFileTempDir();
    assertEquals(untarredFileTempDir, new File(LOCAL_TEMP_DIR, "untarredFileTemp"));
    checkDirExistAndEmpty(untarredFileTempDir);

    File fileDownloadTempDir = provider.getFileDownloadTempDir();
    assertEquals(fileDownloadTempDir, new File(LOCAL_TEMP_DIR, "fileDownloadTemp"));
    checkDirExistAndEmpty(fileDownloadTempDir);

    assertEquals(provider.getVip(), "http://localhost:12345");

    FileUtils.forceDelete(DATA_DIR);
  }

  @Test
  public void testLocalTempDirNotConfigured()
      throws Exception {
    FileUtils.deleteQuietly(DATA_DIR);
    PinotFSFactory.init(new PinotConfiguration());

    ControllerConf controllerConf = new ControllerConf();
    controllerConf.setControllerHost(HOST);
    controllerConf.setControllerPort(PORT);
    controllerConf.setDataDir(DATA_DIR.getPath());
    ControllerFilePathProvider.init(controllerConf);
    ControllerFilePathProvider provider = ControllerFilePathProvider.getInstance();

    URI dataDirURI = provider.getDataDirURI();
    assertEquals(dataDirURI, new URI("file:" + DATA_DIR.getAbsolutePath()));
    assertTrue(new File(dataDirURI).isDirectory());

    File fileUploadTempDir = provider.getFileUploadTempDir();
    assertEquals(fileUploadTempDir, new File(DATA_DIR, "localhost_12345/fileUploadTemp"));
    checkDirExistAndEmpty(fileUploadTempDir);

    File untarredFileTempDir = provider.getUntarredFileTempDir();
    assertEquals(untarredFileTempDir, new File(DATA_DIR, "localhost_12345/untarredFileTemp"));
    checkDirExistAndEmpty(untarredFileTempDir);

    File fileDownloadTempDir = provider.getFileDownloadTempDir();
    assertEquals(fileDownloadTempDir, new File(DATA_DIR, "localhost_12345/fileDownloadTemp"));
    checkDirExistAndEmpty(fileDownloadTempDir);

    assertEquals(provider.getVip(), "http://localhost:12345");

    FileUtils.forceDelete(DATA_DIR);
  }

  @Test
  public void testLocalTempMissing()
      throws Exception {
    FileUtils.deleteQuietly(DATA_DIR);
    PinotFSFactory.init(new PinotConfiguration());

    ControllerConf controllerConf = new ControllerConf();
    controllerConf.setControllerHost(HOST);
    controllerConf.setControllerPort(PORT);
    controllerConf.setDataDir(DATA_DIR.getPath());
    controllerConf.setLocalTempDir(LOCAL_TEMP_DIR.getPath());
    ControllerFilePathProvider.init(controllerConf);
    ControllerFilePathProvider provider = ControllerFilePathProvider.getInstance();

    File fileUploadTempDir = provider.getFileUploadTempDir();
    assertEquals(fileUploadTempDir, new File(LOCAL_TEMP_DIR, "fileUploadTemp"));
    checkDirExistAndEmpty(fileUploadTempDir);

    File untarredFileTempDir = provider.getUntarredFileTempDir();
    assertEquals(untarredFileTempDir, new File(LOCAL_TEMP_DIR, "untarredFileTemp"));
    checkDirExistAndEmpty(untarredFileTempDir);

    File fileDownloadTempDir = provider.getFileDownloadTempDir();
    assertEquals(fileDownloadTempDir, new File(LOCAL_TEMP_DIR, "fileDownloadTemp"));
    checkDirExistAndEmpty(fileDownloadTempDir);

    FileUtils.deleteQuietly(fileUploadTempDir);
    FileUtils.deleteQuietly(untarredFileTempDir);
    FileUtils.deleteQuietly(fileDownloadTempDir);

    fileUploadTempDir = provider.getFileUploadTempDir();
    assertEquals(fileUploadTempDir, new File(LOCAL_TEMP_DIR, "fileUploadTemp"));
    checkDirExistAndEmpty(fileUploadTempDir);

    untarredFileTempDir = provider.getUntarredFileTempDir();
    assertEquals(untarredFileTempDir, new File(LOCAL_TEMP_DIR, "untarredFileTemp"));
    checkDirExistAndEmpty(untarredFileTempDir);

    fileDownloadTempDir = provider.getFileDownloadTempDir();
    assertEquals(fileDownloadTempDir, new File(LOCAL_TEMP_DIR, "fileDownloadTemp"));
    checkDirExistAndEmpty(fileDownloadTempDir);
  }

  private void checkDirExistAndEmpty(File dir) {
    assertTrue(dir.isDirectory());
    String[] children = dir.list();
    assertNotNull(children);
    assertEquals(children.length, 0);
  }
}
