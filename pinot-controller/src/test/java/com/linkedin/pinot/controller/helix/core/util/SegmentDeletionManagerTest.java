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

package com.linkedin.pinot.controller.helix.core.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.controller.helix.core.SegmentDeletionManager;
import junit.framework.Assert;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SegmentDeletionManagerTest {
  final static String tableName = "table";
  final static String clusterName = "mock";

  HelixAdmin makeHelixAdmin() {
    HelixAdmin admin = mock(HelixAdmin.class);
    ExternalView ev = mock(ExternalView.class);
    IdealState is = mock(IdealState.class);
    when(admin.getResourceExternalView(clusterName, tableName)).thenReturn(ev);
    when(admin.getResourceIdealState(clusterName, tableName)).thenReturn(is);

    List<String> segmentsInIs = segmentsInIdealStateOrExtView();
    Map<String, String> dummy = new HashMap<>(1);
    dummy.put("someHost", "ONLINE");
    for (String segment : segmentsInIs) {
      when(is.getInstanceStateMap(segment)).thenReturn(dummy);
    }
    when(ev.getStateMap(anyString())).thenReturn(null);

    return admin;
  }

  ZkHelixPropertyStore<ZNRecord> makePropertyStore() {
    ZkHelixPropertyStore store = mock(ZkHelixPropertyStore.class);
    List<String> failedSegs = segmentsFailingPropStore();
    for (String segment : failedSegs) {
      String propStorePath = ZKMetadataProvider.constructPropertyStorePathForSegment(tableName, segment);
      when(store.remove(propStorePath, AccessOption.PERSISTENT)).thenReturn(false);
    }
    List<String> successfulSegs = segmentsThatShouldBeDeleted();
    for (String segment : successfulSegs) {
      String propStorePath = ZKMetadataProvider.constructPropertyStorePathForSegment(tableName, segment);
      when(store.remove(propStorePath, AccessOption.PERSISTENT)).thenReturn(true);
    }

    when(store.exists(anyString(), anyInt())).thenReturn(true);
    return store;
  }

  List<String> segmentsThatShouldBeDeleted() {
    List<String> result = new ArrayList<>(3);
    result.add("seg1");
    result.add("seg2");
    result.add("seg3");
    return result;
  }

  List<String> segmentsInIdealStateOrExtView() {
    List<String> result = new ArrayList<>(3);
    result.add("seg11");
    result.add("seg12");
    result.add("seg13");
    return result;
  }

  List<String> segmentsFailingPropStore() {
    List<String> result = new ArrayList<>(3);
    result.add("seg21");
    result.add("seg22");
    result.add("seg23");
    return result;
  }

  @Test
  public void testBulkDeleteWithFailures() throws Exception {
    HelixAdmin helixAdmin =  makeHelixAdmin();
    ZkHelixPropertyStore<ZNRecord> propertyStore = makePropertyStore();
    FakeDeletionManager deletionManager = new FakeDeletionManager(helixAdmin, propertyStore);
    List<String> segments = segmentsThatShouldBeDeleted();
    segments.addAll(segmentsInIdealStateOrExtView());
    segments.addAll(segmentsFailingPropStore());
    deletionManager.deleteSegmenetsFromPropertyStoreAndLocal(tableName, segments);

    for (String segment : segmentsFailingPropStore()) {
      Assert.assertTrue(deletionManager.segmentsToRetry.contains(segment));
    }

    for (String segment : segmentsInIdealStateOrExtView()) {
      Assert.assertTrue(deletionManager.segmentsToRetry.contains(segment));
    }

    for (String segment : segmentsThatShouldBeDeleted()) {
      Assert.assertTrue(deletionManager.segmentsRemovedFromStore.contains(segment));
    }
  }

  public static class FakeDeletionManager extends SegmentDeletionManager {

    public Set<String> segmentsRemovedFromStore = new HashSet<>();
    public Set<String> segmentsToRetry = new HashSet<>();

    FakeDeletionManager(HelixAdmin helixAdmin, ZkHelixPropertyStore<ZNRecord> propertyStore) {
      super(null, helixAdmin, clusterName, propertyStore);
    }

    public void deleteSegmenetsFromPropertyStoreAndLocal(String tableName, List<String> segments) {
      super.deleteSegmentFromPropertyStoreAndLocal(tableName, segments, 0L);
    }

    @Override
    protected void removeSegmentFromStore(String tableName, String segmentId) {
      segmentsRemovedFromStore.add(segmentId);
    }
    @Override
    protected void deleteSegmentsWithDelay(final String tableName, final List<String> segmentIds,
        final long deletionDelaySeconds) {
      segmentsToRetry.addAll(segmentIds);
    }
  }
}
