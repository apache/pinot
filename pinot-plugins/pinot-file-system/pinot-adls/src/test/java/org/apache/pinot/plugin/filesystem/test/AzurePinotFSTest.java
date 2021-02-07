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
package org.apache.pinot.plugin.filesystem.test;

import com.microsoft.azure.datalake.store.ADLFileInputStream;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.plugin.filesystem.AzurePinotFS;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests the Azure implementation of PinotFS
 */
public class AzurePinotFSTest {
  private String _adlLocation;
  private File _testFile;

  @BeforeMethod
  public void setup()
      throws IOException {
    _adlLocation =
        new File(System.getProperty("java.io.tmpdir"), AzurePinotFSTest.class.getSimpleName()).getAbsolutePath();
    FileUtils.deleteQuietly(new File(_adlLocation));
    Assert.assertTrue(new File(_adlLocation).mkdir(), "Could not make directory" + _adlLocation);

    try {
      _testFile = new File(_adlLocation, "testFile");
      Assert.assertTrue(_testFile.createNewFile(), "Could not create file " + _testFile.getPath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    new File(_adlLocation).deleteOnExit();
  }

  @Test
  public void testFS()
      throws Exception {
    ADLStoreClient adlStoreClient = Mockito.mock(ADLStoreClient.class);
    ADLFileInputStream adlFileInputStream = Mockito.mock(ADLFileInputStream.class);
    Mockito.when(adlStoreClient.checkExists(_adlLocation)).thenReturn(true);
    Mockito.when(adlStoreClient.checkExists(_testFile.getPath())).thenReturn(true);

    AzurePinotFS azurePinotFS = new AzurePinotFS(adlStoreClient);
    URI testFileURI = _testFile.toURI();
    Assert.assertTrue(azurePinotFS.exists(testFileURI));
    Assert.assertTrue(azurePinotFS.exists(new URI(_adlLocation)));

    File file = new File(_adlLocation, "testfile2");
    Mockito.when(adlStoreClient.getReadStream(ArgumentMatchers.anyString())).thenReturn(adlFileInputStream);
    azurePinotFS.copyToLocalFile(testFileURI, file);
    Assert.assertTrue(file.exists());
  }

  @AfterClass
  public void tearDown() {
    new File(_adlLocation).delete();
  }
}
