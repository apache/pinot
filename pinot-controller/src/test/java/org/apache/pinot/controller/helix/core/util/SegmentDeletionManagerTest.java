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
package org.apache.pinot.controller.helix.core.util;

import com.google.common.io.Files;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.controller.helix.core.SegmentDeletionManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.CommonConstants;
import org.joda.time.DateTime;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SegmentDeletionManagerTest {
  private final static String TABLE_NAME = "table";
  private final static String CLUSTER_NAME = "mock";

  HelixAdmin makeHelixAdmin() {
    HelixAdmin admin = mock(HelixAdmin.class);
    ExternalView ev = mock(ExternalView.class);
    IdealState is = mock(IdealState.class);
    when(admin.getResourceExternalView(CLUSTER_NAME, TABLE_NAME)).thenReturn(ev);
    when(admin.getResourceIdealState(CLUSTER_NAME, TABLE_NAME)).thenReturn(is);

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
    final List<String> failedSegs = segmentsFailingPropStore();
    /*
    for (String segment : failedSegs) {
      String propStorePath = ZKMetadataProvider.constructPropertyStorePathForSegment(tableName, segment);
      when(store.remove(propStorePath, AccessOption.PERSISTENT)).thenReturn(false);
    }
    List<String> successfulSegs = segmentsThatShouldBeDeleted();
    for (String segment : successfulSegs) {
      String propStorePath = ZKMetadataProvider.constructPropertyStorePathForSegment(tableName, segment);
      when(store.remove(propStorePath, AccessOption.PERSISTENT)).thenReturn(true);
    }
    */
    when(store.remove(anyList(), anyInt())).thenAnswer(new Answer<boolean[]>() {
      @Override
      public boolean[] answer(InvocationOnMock invocationOnMock)
          throws Throwable {
        List<String> propStoreList = (List<String>) (invocationOnMock.getArguments()[0]);
        boolean[] result = new boolean[propStoreList.size()];
        for (int i = 0; i < result.length; i++) {
          final String path = propStoreList.get(i);
          final String segmentId = path.substring((path.lastIndexOf('/') + 1));
          if (failedSegs.indexOf(segmentId) < 0) {
            result[i] = true;
          } else {
            result[i] = false;
          }
        }
        return result;
      }
    });

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
  public void testBulkDeleteWithFailures()
      throws Exception {
    testBulkDeleteWithFailures(true);
    testBulkDeleteWithFailures(false);
  }

  public void testBulkDeleteWithFailures(boolean useSet)
      throws Exception {
    HelixAdmin helixAdmin = makeHelixAdmin();
    ZkHelixPropertyStore<ZNRecord> propertyStore = makePropertyStore();
    FakeDeletionManager deletionManager = new FakeDeletionManager(helixAdmin, propertyStore);
    Collection<String> segments;
    if (useSet) {
      segments = new HashSet<>();
    } else {
      segments = new ArrayList<>();
    }

    segments.addAll(segmentsThatShouldBeDeleted());
    segments.addAll(segmentsInIdealStateOrExtView());
    segments.addAll(segmentsFailingPropStore());
    deletionManager.deleteSegmentsFromPropertyStoreAndLocal(TABLE_NAME, segments);

    Assert.assertTrue(deletionManager._segmentsToRetry.containsAll(segmentsFailingPropStore()));
    Assert.assertTrue(deletionManager._segmentsToRetry.containsAll(segmentsInIdealStateOrExtView()));
    Assert.assertTrue(deletionManager._segmentsRemovedFromStore.containsAll(segmentsThatShouldBeDeleted()));
  }

  @Test
  public void testAllFailed() {
    testAllFailed(segmentsFailingPropStore());
    testAllFailed(segmentsInIdealStateOrExtView());
    List<String> segments = segmentsFailingPropStore();
    segments.addAll(segmentsInIdealStateOrExtView());
    testAllFailed(segments);
  }

  @Test
  public void allPassed() {
    HelixAdmin helixAdmin = makeHelixAdmin();
    ZkHelixPropertyStore<ZNRecord> propertyStore = makePropertyStore();
    FakeDeletionManager deletionManager = new FakeDeletionManager(helixAdmin, propertyStore);
    Set<String> segments = new HashSet<>();
    segments.addAll(segmentsThatShouldBeDeleted());
    deletionManager.deleteSegmentsFromPropertyStoreAndLocal(TABLE_NAME, segments);

    Assert.assertEquals(deletionManager._segmentsToRetry.size(), 0);
    Assert.assertEquals(deletionManager._segmentsRemovedFromStore.size(), segments.size());
    Assert.assertTrue(deletionManager._segmentsRemovedFromStore.containsAll(segments));
  }

  private void testAllFailed(List<String> segments) {
    HelixAdmin helixAdmin = makeHelixAdmin();
    ZkHelixPropertyStore<ZNRecord> propertyStore = makePropertyStore();
    FakeDeletionManager deletionManager = new FakeDeletionManager(helixAdmin, propertyStore);
    deletionManager.deleteSegmentsFromPropertyStoreAndLocal(TABLE_NAME, segments);

    Assert.assertTrue(deletionManager._segmentsToRetry.containsAll(segments));
    Assert.assertEquals(deletionManager._segmentsToRetry.size(), segments.size());
    Assert.assertEquals(deletionManager._segmentsRemovedFromStore.size(), 0);
  }

  @Test
  public void testRemoveDeletedSegments()
      throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Controller.PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY + ".class",
        LocalPinotFS.class.getName());
    PinotFSFactory.init(new PinotConfiguration(properties));

    HelixAdmin helixAdmin = makeHelixAdmin();
    ZkHelixPropertyStore<ZNRecord> propertyStore = makePropertyStore();
    File tempDir = Files.createTempDir();
    tempDir.deleteOnExit();
    FakeDeletionManager deletionManager = new FakeDeletionManager(tempDir.getAbsolutePath(), helixAdmin, propertyStore);

    // Test delete when deleted segments directory does not exists
    deletionManager.removeAgedDeletedSegments(1);

    // Create deleted directory
    String deletedDirectoryPath = tempDir + File.separator + "Deleted_Segments";
    File deletedDirectory = new File(deletedDirectoryPath);
    deletedDirectory.mkdir();

    // Test delete when deleted segments directory is empty
    deletionManager.removeAgedDeletedSegments(1);

    // Create dummy directories and files
    File dummyDir1 = new File(deletedDirectoryPath + File.separator + "dummy1");
    dummyDir1.mkdir();
    File dummyDir2 = new File(deletedDirectoryPath + File.separator + "dummy2");
    dummyDir2.mkdir();

    // Test delete when there is no files but some directories exist
    deletionManager.removeAgedDeletedSegments(1);
    Assert.assertEquals(dummyDir1.exists(), false);
    Assert.assertEquals(dummyDir2.exists(), false);

    // Create dummy directories and files
    dummyDir1.mkdir();
    dummyDir2.mkdir();

    // Create dummy files
    for (int i = 0; i < 3; i++) {
      createTestFileWithAge(dummyDir1.getAbsolutePath() + File.separator + "file" + i, i);
    }
    for (int i = 2; i < 5; i++) {
      createTestFileWithAge(dummyDir2.getAbsolutePath() + File.separator + "file" + i, i);
    }

    // Sleep 1 second to ensure the clock moves.
    Thread.sleep(1000L);

    // Check that dummy directories and files are successfully created.
    Assert.assertEquals(dummyDir1.list().length, 3);
    Assert.assertEquals(dummyDir2.list().length, 3);

    // Try to remove files with the retention of 3 days.
    deletionManager.removeAgedDeletedSegments(3);
    Assert.assertEquals(dummyDir1.list().length, 3);
    Assert.assertEquals(dummyDir2.list().length, 1);

    // Try to further remove files with the retention of 1 days.
    deletionManager.removeAgedDeletedSegments(1);
    Assert.assertEquals(dummyDir1.list().length, 1);

    // Check that empty directory has successfully been removed.
    Assert.assertEquals(dummyDir2.exists(), false);
  }

  public void createTestFileWithAge(String path, int age)
      throws Exception {
    File testFile = new File(path);
    testFile.createNewFile();
    testFile.setLastModified(DateTime.now().minusDays(age).getMillis());
  }

  public static class FakeDeletionManager extends SegmentDeletionManager {

    public Set<String> _segmentsRemovedFromStore = new HashSet<>();
    public Set<String> _segmentsToRetry = new HashSet<>();

    FakeDeletionManager(HelixAdmin helixAdmin, ZkHelixPropertyStore<ZNRecord> propertyStore) {
      super(null, helixAdmin, CLUSTER_NAME, propertyStore);
    }

    FakeDeletionManager(String localDiskDir, HelixAdmin helixAdmin, ZkHelixPropertyStore<ZNRecord> propertyStore) {
      super(localDiskDir, helixAdmin, CLUSTER_NAME, propertyStore);
    }

    public void deleteSegmentsFromPropertyStoreAndLocal(String tableName, Collection<String> segments) {
      super.deleteSegmentFromPropertyStoreAndLocal(tableName, segments, 0L);
    }

    @Override
    protected void removeSegmentFromStore(String tableName, String segmentId) {
      _segmentsRemovedFromStore.add(segmentId);
    }

    @Override
    protected void deleteSegmentsWithDelay(final String tableName, final Collection<String> segmentIds,
        final long deletionDelaySeconds) {
      _segmentsToRetry.addAll(segmentIds);
    }
  }
}
