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
package org.apache.pinot.plugin.ingestion.batch.hadoop;

import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.utils.SegmentPushUtils;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;


/// Tests driver-side archive selection with scoped mocks and without starting Hadoop.
public class HadoopSegmentMetadataPushJobRunnerTest {
  @Test
  public void testPushPatternFiltersBeforeDuplicateValidation()
      throws Exception {
    URI outputDir = URI.create("file:/output/");
    String selected = "file:/output/current/segment.tar.zst";
    PinotFS fileSystem = mock(PinotFS.class);
    when(fileSystem.listFiles(outputDir, true)).thenReturn(new String[]{
        selected, "file:/output/current/segment.tar.gz", "file:/output/stale/segment.tar.zst"});
    SegmentGenerationJobSpec spec = new SegmentGenerationJobSpec();
    spec.setOutputDirURI(outputDir.toString());
    spec.setPinotFSSpecs(List.of());
    PushJobSpec pushSpec = new PushJobSpec();
    pushSpec.setPushFileNamePattern("glob:**/current/*.tar.zst");
    spec.setPushJobSpec(pushSpec);
    HadoopSegmentMetadataPushJobRunner runner = new HadoopSegmentMetadataPushJobRunner(spec);

    try (MockedStatic<PinotFSFactory> fileSystems = mockStatic(PinotFSFactory.class);
        MockedStatic<SegmentPushUtils> pushes = mockStatic(SegmentPushUtils.class)) {
      fileSystems.when(() -> PinotFSFactory.create("file")).thenReturn(fileSystem);
      pushes.when(() -> SegmentPushUtils.getSegmentUriToTarPathMap(eq(outputDir), eq(pushSpec), any(String[].class)))
          .thenCallRealMethod();
      pushes.when(() -> SegmentPushUtils.validateUniqueSegmentNames(anyCollection())).thenCallRealMethod();
      pushes.when(() -> SegmentPushUtils.getSegmentNameFromTarFile(anyString())).thenCallRealMethod();
      pushes.when(() -> SegmentPushUtils.generateSegmentTarURI(eq(outputDir), any(URI.class), any(), any()))
          .thenCallRealMethod();
      runner.run();
      pushes.verify(() -> SegmentPushUtils.sendSegmentUriAndMetadata(spec, fileSystem, Map.of(selected, selected)));

      pushSpec.setPushParallelism(0);
      pushSpec.setPushFileNamePattern("glob:**/missing/*");
      runner.run();

      pushSpec.setPushFileNamePattern(null);
      assertThrows(IllegalArgumentException.class, runner::run);
      pushes.verify(() -> SegmentPushUtils.sendSegmentUriAndMetadata(eq(spec), eq(fileSystem), anyMap()), times(1));
    }
  }
}
