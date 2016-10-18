/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import java.io.File;

import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.Schema;
/**
 * enables indexes on a bunch of columns
 *
 */
@Test
public class InvertedIndexOfflineIntegrationTest extends OfflineClusterIntegrationTest{
  @Override
  protected void setUpTable(File schemaFile, int numBroker, int numOffline) throws Exception {
    addSchema(schemaFile, "schemaFile");
    Schema schema = Schema.fromFile(schemaFile);
    addOfflineTable("DaysSinceEpoch", "daysSinceEpoch", -1, "", null, null, schema.getDimensionNames(), null, "mytable",
        SegmentVersion.v1);
  }

}
