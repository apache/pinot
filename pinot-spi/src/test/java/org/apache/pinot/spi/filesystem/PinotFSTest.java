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
package org.apache.pinot.spi.filesystem;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PinotFSTest {
  public String srcForMoveFile = "myfs://root/file1";
  public String dstForMoveFile = "myfs://root/someDir/file1";
  public String srcForMoveFileWithPort = "myfs://myhost1:1234/root/file1";
  public String dstForMoveFileWithPort = "myfs://myhost2:1234/someDir/file1";

  @Test
  public void testMoveFileUriGeneration() throws Exception {
    MockRemoteFS fs = new MockRemoteFS();
    fs.init(null);
    fs.move(new URI(srcForMoveFile), new URI(dstForMoveFile), false);

    Assert.assertEquals(fs.mkdirCalls, 1,"should call mkdir once");
    Assert.assertEquals(fs.mkdirArgs.get(0).toString(), "myfs://root/someDir", "should create correct parent");

    Assert.assertEquals(fs.doMoveCalls, 1, "doMove should be called once");
    Map<String, URI> callArgs = fs.doMoveArgs.get(0);
    Assert.assertEquals(callArgs.get("srcUri").toString(), srcForMoveFile, "should keep correct src");
    Assert.assertEquals(callArgs.get("dstUri").toString(), dstForMoveFile, "should keep correct dst");
  }

  @Test
  public void testMoveFileUriGenerationWithPort() throws Exception {
    MockRemoteFS fs = new MockRemoteFS();
    fs.init(null);
    fs.move(new URI(srcForMoveFileWithPort), new URI(dstForMoveFileWithPort), false);

    Assert.assertEquals(fs.mkdirCalls, 1,"should call mkdir once");
    Assert.assertEquals(fs.mkdirArgs.get(0).toString(), "myfs://myhost2:1234/someDir", "should create correct parent");

    Assert.assertEquals(fs.doMoveCalls, 1, "doMove should be called once");
    Map<String, URI> callArgs = fs.doMoveArgs.get(0);
    Assert.assertEquals(callArgs.get("srcUri").toString(), srcForMoveFileWithPort, "should keep correct src");
    Assert.assertEquals(callArgs.get("dstUri").toString(), dstForMoveFileWithPort, "should keep correct dst");
  }

  /**
   * MockRemoteFS implementation used to test behavior of the Abstract class PinotFS
   */
  private class MockRemoteFS extends PinotFS {
    public int doMoveCalls;
    public List<Map<String, URI>> doMoveArgs;

    public int mkdirCalls;
    public List<URI> mkdirArgs;

    @Override
    public void init(PinotConfiguration config) {
      doMoveCalls = 0;
      doMoveArgs = new ArrayList<>();

      mkdirCalls = 0;
      mkdirArgs = new ArrayList<>();
    }

    @Override
    public boolean mkdir(URI uri)
        throws IOException {
      mkdirArgs.add(uri);
      mkdirCalls++;
      return true;
    }

    @Override
    public boolean delete(URI segmentUri, boolean forceDelete)
        throws IOException {
      return false;
    }

    @Override
    public boolean doMove(URI srcUri, URI dstUri)
        throws IOException {
      HashMap<String, URI> call = new HashMap<>();
      call.put("srcUri", srcUri);
      call.put("dstUri", dstUri);
      doMoveArgs.add(call);
      doMoveCalls++;
      return true;
    }

    @Override
    public boolean copy(URI srcUri, URI dstUri)
        throws IOException {
      return false;
    }

    @Override
    public boolean exists(URI fileUri)
        throws IOException {

      if (fileUri.toString() == srcForMoveFile ||
          fileUri.toString() == srcForMoveFileWithPort) {
        return true;
      }

      return false;
    }

    @Override
    public long length(URI fileUri)
        throws IOException {
      return 0;
    }

    @Override
    public String[] listFiles(URI fileUri, boolean recursive)
        throws IOException {
      return new String[0];
    }

    @Override
    public void copyToLocalFile(URI srcUri, File dstFile)
        throws Exception {

    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri)
        throws Exception {

    }

    @Override
    public boolean isDirectory(URI uri)
        throws IOException {
      return false;
    }

    @Override
    public long lastModified(URI uri)
        throws IOException {
      return 0;
    }

    @Override
    public boolean touch(URI uri)
        throws IOException {
      return false;
    }

    @Override
    public InputStream open(URI uri)
        throws IOException {
      return null;
    }
  }
}
