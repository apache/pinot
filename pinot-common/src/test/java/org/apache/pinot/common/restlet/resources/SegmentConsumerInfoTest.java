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


package org.apache.pinot.common.restlet.resources;

import java.util.Collections;
import java.util.Map;
import org.apache.pinot.spi.stream.ConsumerPartitionState;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.RowMetadata;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

public class SegmentConsumerInfoTest {
    @Test
    public void testMissingUpstreamOffset() {
        Map<String, String> currentOffsets = Collections.singletonMap("0", "5000000");
        RowMetadata rowMetadata = mock(RowMetadata.class);
        Map<String, ConsumerPartitionState> partitionStateMap = Collections.singletonMap("0",
                new ConsumerPartitionState("0", new LongMsgOffset(5000000), 1676890508681L, null, rowMetadata));
        Map<String, String> recordsLag = Collections.singletonMap("0", "UNKNOWN");
        Map<String, String> availabilityLagMs = Collections.singletonMap("0", "11605");
        SegmentConsumerInfo.PartitionOffsetInfo offsetInfo = SegmentConsumerInfo.PartitionOffsetInfo.createFrom(
                currentOffsets, partitionStateMap, recordsLag, availabilityLagMs
        );

        Map<String, String> latestUpstreamOffsets = offsetInfo.getLatestUpstreamOffsets();
        assertEquals(latestUpstreamOffsets.get("0"), "UNKNOWN");
    }
}
