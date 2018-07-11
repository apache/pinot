/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.broker.pruner;

import com.linkedin.pinot.common.metadata.segment.ColumnPartitionMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentPartitionMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.apache.commons.lang.math.IntRange;
import org.testng.annotations.Test;


/**
 * Unit test for {@link SegmentZKMetadataPruner}
 */
public class SegmentZKMetadataPrunerTest {

  private static final int NUM_PARTITION = 11;
  private static final String PARTITION_COLUMN = "partition";
  private static final String PARTITION_FUNCTION_NAME = "modulo";
  private static final String PRUNER_NAME = "partitionzkmetadatapruner";

  @Test
  public void testPruner() {
    SegmentZKMetadata metadata = new OfflineSegmentZKMetadata();
    Map<String, ColumnPartitionMetadata> columnPartitionMap = new HashMap<>();

    int expectedPartition = 3;
    columnPartitionMap.put(PARTITION_COLUMN, new ColumnPartitionMetadata(PARTITION_FUNCTION_NAME, NUM_PARTITION,
        Collections.singletonList(new IntRange(expectedPartition))));

    SegmentZKMetadataPrunerService prunerService = new SegmentZKMetadataPrunerService(new String[]{PRUNER_NAME});
    SegmentPartitionMetadata segmentPartitionMetadata = new SegmentPartitionMetadata(columnPartitionMap);
    metadata.setPartitionMetadata(segmentPartitionMetadata);

    Pql2Compiler compiler = new Pql2Compiler();
    for (int actualPartition = 0; actualPartition < NUM_PARTITION; actualPartition++) {
      String query = "select count(*) from myTable where " + PARTITION_COLUMN + " = " + actualPartition;
      BrokerRequest brokerRequest = compiler.compileToBrokerRequest(query);
      SegmentPrunerContext prunerContext = new SegmentPrunerContext(brokerRequest);
      Assert.assertEquals(prunerService.prune(metadata, prunerContext), (actualPartition != expectedPartition));
    }
  }
}
