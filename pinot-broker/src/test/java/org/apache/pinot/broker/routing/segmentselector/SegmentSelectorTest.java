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
package org.apache.pinot.broker.routing.segmentselector;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentBrokerView;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.HLCSegmentName;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.testng.annotations.Test;

import static org.apache.pinot.broker.routing.segmentselector.RealtimeSegmentSelector.FORCE_HLC;
import static org.apache.pinot.broker.routing.segmentselector.RealtimeSegmentSelector.ROUTING_OPTIONS_KEY;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertTrue;


public class SegmentSelectorTest {

  @Test
  public void testSegmentSelectorFactory() {
    TableConfig tableConfig = mock(TableConfig.class);

    when(tableConfig.getTableType()).thenReturn(TableType.OFFLINE);
    assertTrue(SegmentSelectorFactory.getSegmentSelector(tableConfig) instanceof OfflineSegmentSelector);

    when(tableConfig.getTableType()).thenReturn(TableType.REALTIME);
    assertTrue(SegmentSelectorFactory.getSegmentSelector(tableConfig) instanceof RealtimeSegmentSelector);
  }

  @Test
  public void testRealtimeSegmentSelector() {
    String realtimeTableName = "testTable_REALTIME";
    ExternalView externalView = new ExternalView(realtimeTableName);
    Map<String, Map<String, String>> segmentAssignment = externalView.getRecord().getMapFields();
    Map<String, String> onlineInstanceStateMap = Collections.singletonMap("server", ONLINE);
    Map<String, String> consumingInstanceStateMap = Collections.singletonMap("server", CONSUMING);
    Set<SegmentBrokerView> onlineSegments = new HashSet<>();
    // NOTE: Ideal state is not used in the current implementation.
    IdealState idealState = mock(IdealState.class);

    // Should return an empty list when there is no segment
    RealtimeSegmentSelector segmentSelector = new RealtimeSegmentSelector();
    segmentSelector.init(externalView, idealState, onlineSegments);
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    assertTrue(segmentSelector.select(brokerRequest).isEmpty());

    // For HLC segments, only one group of segments should be selected
    int numHLCGroups = 3;
    int numHLCSegmentsPerGroup = 5;
    String[][] hlcSegments = new String[numHLCGroups][];
    for (int i = 0; i < numHLCGroups; i++) {
      String groupId = "testTable_REALTIME_" + i;
      String[] hlcSegmentsForGroup = new String[numHLCSegmentsPerGroup];
      for (int j = 0; j < numHLCSegmentsPerGroup; j++) {
        String hlcSegment = new HLCSegmentName(groupId, "0", Integer.toString(j)).getSegmentName();
        segmentAssignment.put(hlcSegment, onlineInstanceStateMap);
        onlineSegments.add(new SegmentBrokerView(hlcSegment));
        hlcSegmentsForGroup[j] = hlcSegment;
      }
      hlcSegments[i] = hlcSegmentsForGroup;
    }
    segmentSelector.onExternalViewChange(externalView, idealState, onlineSegments);

    // Only HLC segments exist, should select the HLC segments from the first group
    assertEqualsNoOrder(segmentSelector.select(brokerRequest).stream().map(SegmentBrokerView::getSegmentName).toArray(),
        hlcSegments[0]);

    // For LLC segments, only the first CONSUMING segment for each partition should be selected
    int numLLCPartitions = 3;
    int numLLCSegmentsPerPartition = 5;
    int numOnlineLLCSegmentsPerPartition = 3;
    String[] expectedSelectedLLCSegmentNames = new String[numLLCPartitions * (numLLCSegmentsPerPartition - 1)];
    for (int i = 0; i < numLLCPartitions; i++) {
      for (int j = 0; j < numLLCSegmentsPerPartition; j++) {
        String llcSegment = new LLCSegmentName(realtimeTableName, i, j, 0).getSegmentName();
        if (j < numOnlineLLCSegmentsPerPartition) {
          externalView.setStateMap(llcSegment, onlineInstanceStateMap);
        } else {
          externalView.setStateMap(llcSegment, consumingInstanceStateMap);
        }
        onlineSegments.add(new SegmentBrokerView(llcSegment));
        if (j < numLLCSegmentsPerPartition - 1) {
          expectedSelectedLLCSegmentNames[i * (numLLCSegmentsPerPartition - 1) + j] = llcSegment;
        }
      }
    }
    segmentSelector.onExternalViewChange(externalView, idealState, onlineSegments);
    List<SegmentBrokerView> expectedSelectedLLCSegments =
        Arrays.stream(expectedSelectedLLCSegmentNames).map(SegmentBrokerView::new).collect(Collectors.toList());
    // Both HLC and LLC segments exist, should select the LLC segments
    assertEqualsNoOrder(segmentSelector.select(brokerRequest).toArray(), expectedSelectedLLCSegments.toArray());

    // When HLC is forced, should select the HLC segments from the second group
    when(brokerRequest.getDebugOptions()).thenReturn(Collections.singletonMap(ROUTING_OPTIONS_KEY, FORCE_HLC));
    assertEqualsNoOrder(segmentSelector.select(brokerRequest).stream().map(SegmentBrokerView::getSegmentName).toArray(),
        hlcSegments[1]);

    // Remove all the HLC segments from ideal state, should select the LLC segments even when HLC is forced
    for (String[] hlcSegmentsForGroup : hlcSegments) {
      for (String hlcSegment : hlcSegmentsForGroup) {
        onlineSegments.remove(new SegmentBrokerView(hlcSegment));
      }
    }
    segmentSelector.onExternalViewChange(externalView, idealState, onlineSegments);
    assertEqualsNoOrder(segmentSelector.select(brokerRequest).toArray(), expectedSelectedLLCSegments.toArray());
  }
}
