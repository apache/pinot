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
package org.apache.pinot.tools.admin.command;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests archive discovery for backfill replacement lineage.
public class LaunchBackfillIngestionJobCommandTest {
  @Test
  public void testCollectSegmentNames()
      throws Exception {
    File outputDir = Files.createTempDirectory("backfill segments ").toFile();
    try {
      File nestedDir = new File(outputDir, "nested.tar.gz");
      FileUtils.forceMkdir(nestedDir);
      for (String file : List.of("gzip.tar.gz", "zstd.tar.zst", "lz4.tar.lz4", "gzip.metadata.tar.gz", "input.csv")) {
        FileUtils.touch(new File(nestedDir, file));
      }
      FileUtils.forceMkdir(new File(nestedDir, "directory.tar.zst"));
      SegmentGenerationJobSpec spec = new SegmentGenerationJobSpec();
      spec.setOutputDirURI(outputDir.toURI().toString());
      LaunchBackfillIngestionJobCommand command = new LaunchBackfillIngestionJobCommand();
      assertEquals(Set.copyOf(command.collectSegmentNames(spec)), Set.of("gzip", "zstd", "lz4"));

      PushJobSpec pushSpec = new PushJobSpec();
      pushSpec.setPushFileNamePattern("glob:**/*.tar.zst");
      spec.setPushJobSpec(pushSpec);
      assertEquals(command.collectSegmentNames(spec), List.of("zstd"));
    } finally {
      FileUtils.deleteDirectory(outputDir);
    }
  }
}
