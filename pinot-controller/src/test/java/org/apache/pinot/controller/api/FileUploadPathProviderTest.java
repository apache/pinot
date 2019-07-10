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
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.resources.FileUploadPathProvider;
import org.apache.pinot.controller.helix.ControllerTest;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class FileUploadPathProviderTest extends ControllerTest {
  private static ControllerConf _controllerConf;

  @BeforeClass
  public void setUp() {
    startZk();
    _controllerConf = getDefaultControllerConfiguration();
    startController(_controllerConf);
  }

  @Test
  public void testFileUploadPathProvider()
      throws Exception {
    FileUploadPathProvider provider = new FileUploadPathProvider(_controllerConf);
    Assert.assertEquals(provider.getBaseDataDir().getAbsolutePath(), _controllerDataDir);
    String fileUploadTmpDirPath = provider.getFileUploadTmpDir().getAbsolutePath();
    Assert.assertEquals(fileUploadTmpDirPath, new File(_controllerDataDir, "fileUploadTemp").getAbsolutePath());
    Assert.assertEquals(provider.getSchemasTmpDir().getAbsolutePath(),
        new File(_controllerDataDir, "schemasTemp").getAbsolutePath());
    Assert.assertEquals(provider.getTmpUntarredPath().getAbsolutePath(),
        new File(fileUploadTmpDirPath, "untarred").getAbsolutePath());
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
