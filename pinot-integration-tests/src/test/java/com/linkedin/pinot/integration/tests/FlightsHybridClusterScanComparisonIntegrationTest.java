/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.io.IOException;
import org.apache.commons.compress.archivers.ArchiveException;
import org.testng.annotations.Test;


/**
 * Hybrid cluster scan comparison integration test for the flights data set.
 */
public class FlightsHybridClusterScanComparisonIntegrationTest extends HybridClusterScanComparisonIntegrationTest {
  @Override
  @Test(enabled = false) // jfim: This should already be covered by our tests against H2
  public void testGeneratedQueries() throws Exception {
    super.testGeneratedQueries();
  }

  @Override
  protected String getTimeColumnName() {
    return "DaysSinceEpoch";
  }

  @Override
  protected String getTimeColumnType() {
    return "DAYS";
  }

  @Override
  protected String getSortedColumn() {
    return "DaysSinceEpoch";
  }

  @Override
  protected void extractAvroIfNeeded() throws IOException {
    // Unpack the Avro files
    try {
      TarGzCompressionUtils.unTar(
          new File(TestUtils.getFileFromResourceUrl(OfflineClusterIntegrationTest.class.getClassLoader().getResource(
              "On_Time_On_Time_Performance_2014_100k_subset_nonulls.tar.gz"))), _tmpDir);
    } catch (ArchiveException e) {
      throw new IOException("Caught exception while unpacking Avro files", e);
    }
  }

  @Override
  protected int getAvroFileCount() {
    return 12;
  }
}
