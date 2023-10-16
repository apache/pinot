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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.SegmentDeletionManager;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
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
  private static final String TABLE_NAME = "table";
  private static final String CLUSTER_NAME = "mock";
  // these prefix must be the same as those in SegmentDeletionManager.
  private static final String RETENTION_UNTIL_SEPARATOR = "__RETENTION_UNTIL__";
  private static final String RETENTION_DATE_FORMAT_STR = "yyyyMMddHHmm";
  private static final SimpleDateFormat RETENTION_DATE_FORMAT;

  static {
    RETENTION_DATE_FORMAT = new SimpleDateFormat(RETENTION_DATE_FORMAT_STR);
    RETENTION_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

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
    FakeDeletionManager deletionManager = new FakeDeletionManager(
        tempDir.getAbsolutePath(), helixAdmin, propertyStore, 7);
    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);

    // Test delete when deleted segments directory does not exists
    deletionManager.removeAgedDeletedSegments(leadControllerManager);

    // Create deleted directory
    String deletedDirectoryPath = tempDir + File.separator + "Deleted_Segments";
    File deletedDirectory = new File(deletedDirectoryPath);
    deletedDirectory.mkdir();

    // Test delete when deleted segments directory is empty
    deletionManager.removeAgedDeletedSegments(leadControllerManager);

    // Create dummy directories and files
    File dummyDir1 = new File(deletedDirectoryPath + File.separator + "dummy1 %");
    dummyDir1.mkdir();
    File dummyDir2 = new File(deletedDirectoryPath + File.separator + "dummy2 %");
    dummyDir2.mkdir();
    File dummyDir3 = new File(deletedDirectoryPath + File.separator + "dummy3 %");
    dummyDir3.mkdir();

    // Test delete when there is no files but some directories exist
    deletionManager.removeAgedDeletedSegments(leadControllerManager);
    Assert.assertEquals(dummyDir1.exists(), false);
    Assert.assertEquals(dummyDir2.exists(), false);
    Assert.assertEquals(dummyDir3.exists(), false);

    // Create dummy directories and files again
    dummyDir1.mkdir();
    dummyDir2.mkdir();
    dummyDir3.mkdir();

    // Create dummy files
    for (int i = 0; i < 3; i++) {
      createTestFileWithAge(dummyDir1.getAbsolutePath() + File.separator + genDeletedSegmentName("file %" + i, i, 1),
          i);
    }
    for (int i = 2; i < 5; i++) {
      createTestFileWithAge(dummyDir2.getAbsolutePath() + File.separator + genDeletedSegmentName("file %" + i, i, 1),
          i);
    }
    for (int i = 6; i < 9; i++) {
      createTestFileWithAge(dummyDir3.getAbsolutePath() + File.separator + "file %" + i, i);
    }

    // Sleep 1 second to ensure the clock moves.
    Thread.sleep(1000L);

    // Check that dummy directories and files are successfully created.
    Assert.assertEquals(dummyDir1.list().length, 3);
    Assert.assertEquals(dummyDir2.list().length, 3);
    Assert.assertEquals(dummyDir3.list().length, 3);

    // Try to remove files with the retention of 1 days.
    deletionManager.removeAgedDeletedSegments(leadControllerManager);

    // Check that only 1 day retention file is remaining
    Assert.assertEquals(dummyDir1.list().length, 1);

    // Check that empty directory has successfully been removed.
    Assert.assertEquals(dummyDir2.exists(), false);

    // Check that deleted file without retention suffix is honoring cluster-wide retention period of 7 days.
    Assert.assertEquals(dummyDir3.list().length, 1);
  }

  @Test
  public void testSegmentDeletionLogic()
      throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Controller.PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY + ".class",
        LocalPinotFS.class.getName());
    PinotFSFactory.init(new PinotConfiguration(properties));

    HelixAdmin helixAdmin = makeHelixAdmin();
    ZkHelixPropertyStore<ZNRecord> propertyStore = makePropertyStore();
    File tempDir = Files.createTempDir();
    tempDir.deleteOnExit();
    SegmentDeletionManager deletionManager = new SegmentDeletionManager(
        tempDir.getAbsolutePath(), helixAdmin, CLUSTER_NAME, propertyStore, 7);

    // create table segment files.
    Set<String> segments = new HashSet<>(segmentsThatShouldBeDeleted());
    createTableAndSegmentFiles(tempDir, segmentsThatShouldBeDeleted());
    final File tableDir = new File(tempDir.getAbsolutePath() + File.separator + TABLE_NAME);
    final File deletedTableDir = new File(tempDir.getAbsolutePath() + File.separator + "Deleted_Segments"
        + File.separator + TABLE_NAME);

    // delete the segments instantly.
    SegmentsValidationAndRetentionConfig mockValidationConfig = mock(SegmentsValidationAndRetentionConfig.class);
    when(mockValidationConfig.getDeletedSegmentsRetentionPeriod()).thenReturn("0d");
    TableConfig mockTableConfig = mock(TableConfig.class);
    when(mockTableConfig.getValidationConfig()).thenReturn(mockValidationConfig);
    deletionManager.deleteSegments(TABLE_NAME, segments, mockTableConfig);

    TestUtils.waitForCondition(aVoid -> {
      try {
        Assert.assertEquals(tableDir.listFiles().length, 0);
        Assert.assertTrue(!deletedTableDir.exists() || deletedTableDir.listFiles().length == 0);
        return true;
      } catch (Throwable t) {
        return false;
      }
    }, 2000L, 10_000L, "Unable to verify table deletion with retention");

    // create table segment files again to test default retention.
    createTableAndSegmentFiles(tempDir, segmentsThatShouldBeDeleted());
    // delete the segments with default retention
    deletionManager.deleteSegments(TABLE_NAME, segments);

    TestUtils.waitForCondition(aVoid -> {
      try {
        Assert.assertEquals(tableDir.listFiles().length, 0);
        Assert.assertEquals(deletedTableDir.listFiles().length, segments.size());
        return true;
      } catch (Throwable t) {
        return false;
      }
    }, 2000L, 10_000L, "Unable to verify table deletion with retention");
  }

  public void createTableAndSegmentFiles(File tempDir, List<String> segmentIds)
      throws Exception {
    File tableDir = new File(tempDir.getAbsolutePath() + File.separator + TABLE_NAME);
    tableDir.mkdir();
    for (String segmentId : segmentIds) {
      createTestFileWithAge(tableDir.getAbsolutePath() + File.separator + segmentId, 0);
    }
  }

  public String genDeletedSegmentName(String fileName, int age, int retentionInDays) {
    // adding one more hours to the deletion time just to make sure the test goes pass the retention period because
    // we no longer keep second level info in the date format.
    return StringUtils.join(fileName, RETENTION_UNTIL_SEPARATOR, RETENTION_DATE_FORMAT.format(new Date(
        DateTime.now().minusDays(age).getMillis()
            + TimeUnit.DAYS.toMillis(retentionInDays)
            - TimeUnit.HOURS.toMillis(1))));
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
      super(null, helixAdmin, CLUSTER_NAME, propertyStore, 0);
    }

    FakeDeletionManager(String localDiskDir, HelixAdmin helixAdmin, ZkHelixPropertyStore<ZNRecord> propertyStore, int
        deletedSegmentsRetentionInDays) {
      super(localDiskDir, helixAdmin, CLUSTER_NAME, propertyStore, deletedSegmentsRetentionInDays);
    }

    public void deleteSegmentsFromPropertyStoreAndLocal(String tableName, Collection<String> segments) {
      super.deleteSegmentFromPropertyStoreAndLocal(tableName, segments, 0L, 0L);
    }

    @Override
    protected void removeSegmentFromStore(String tableName, String segmentId,
        @Nullable Long deletedSegmentsRetentionMs) {
      _segmentsRemovedFromStore.add(segmentId);
    }

    @Override
    protected void deleteSegmentsWithDelay(String tableName, Collection<String> segmentIds,
        @Nullable Long deletedSegmentsRetentionMs, long deletionDelaySeconds) {
      _segmentsToRetry.addAll(segmentIds);
    }
  }
}
