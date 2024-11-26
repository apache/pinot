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
package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.forward.CLPMutableForwardIndexV2;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.CLPForwardIndexCreatorV2;
import org.apache.pinot.segment.local.segment.index.forward.mutable.VarByteSVMutableForwardIndexTest;
import org.apache.pinot.segment.local.segment.index.readers.forward.CLPForwardIndexReaderV2;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class CLPForwardIndexCreatorV2Test {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), CLPForwardIndexCreatorV2Test.class.getSimpleName());
  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(TEMP_DIR);
    _memoryManager = new DirectMemoryManager(VarByteSVMutableForwardIndexTest.class.getName());
  }

  @Test
  public void testCLPWriter()
      throws IOException {
    List<String> logLines = new ArrayList<>();
    logLines.add("INFO [PropertyCache] [HelixController-pipeline-default-pinot-(4a02a32c_DEFAULT)] "
        + "Event pinot::DEFAULT::4a02a32c_DEFAULT : Refreshed 35 property LiveInstance took 5 ms. Selective: true");
    logLines.add("INFO [PropertyCache] [HelixController-pipeline-default-pinot-(4a02a32d_DEFAULT)] "
        + "Event pinot::DEFAULT::4a02a32d_DEFAULT : Refreshed 81 property LiveInstance took 4 ms. Selective: true");
    logLines.add("INFO [ControllerResponseFilter] [grizzly-http-server-2] Handled request from 0.0"
        + ".0.0 GET https://0.0.0.0:8443/health?checkType=liveness, content-type null status code 200 OK");
    logLines.add("INFO [ControllerResponseFilter] [grizzly-http-server-6] Handled request from 0.0"
        + ".0.0 GET https://pinot-pinot-broker-headless.managed.svc.cluster.local:8093/tables, content-type "
        + "application/json status code 200 OK");
    logLines.add("null");

    // Create and ingest into a clp mutable forward indexes
    CLPMutableForwardIndexV2 clpMutableForwardIndexV2 = new CLPMutableForwardIndexV2("column1", _memoryManager);
    for (int i = 0; i < logLines.size(); i++) {
      clpMutableForwardIndexV2.setString(i, logLines.get(i));
    }

    // Create a immutable forward index from mutable forward index
    CLPForwardIndexCreatorV2 clpForwardIndexCreatorV2 =
        new CLPForwardIndexCreatorV2(TEMP_DIR, clpMutableForwardIndexV2, ChunkCompressionType.ZSTANDARD);
    for (int i = 0; i < logLines.size(); i++) {
      clpForwardIndexCreatorV2.putString(clpMutableForwardIndexV2.getString(i));
    }
    clpForwardIndexCreatorV2.seal();
    clpForwardIndexCreatorV2.close();

    // Read from immutable forward index and validate the content
    File indexFile = new File(TEMP_DIR, "column1" + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
    PinotDataBuffer pinotDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    CLPForwardIndexReaderV2 clpForwardIndexReaderV2 = new CLPForwardIndexReaderV2(pinotDataBuffer, logLines.size());
    CLPForwardIndexReaderV2.CLPReaderContext clpForwardIndexReaderV2Context = clpForwardIndexReaderV2.createContext();
    for (int i = 0; i < logLines.size(); i++) {
      Assert.assertEquals(clpForwardIndexReaderV2.getString(i, clpForwardIndexReaderV2Context), logLines.get(i));
    }
  }
}
