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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.SegmentDeletionManager;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.joda.time.DateTime;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.common.metadata.ZKMetadataProvider.constructPropertyStorePathForSegment;
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

  protected SegmentDeletionManager createDeletionManager(String dataDir, HelixAdmin helixAdmin, String clusterName,
      ZkHelixPropertyStore<ZNRecord> propertyStore, int deletedSegmentsRetentionInDays) {
    return new SegmentDeletionManager(dataDir, helixAdmin, clusterName, propertyStore, deletedSegmentsRetentionInDays);
  }

  private static final String METADATA_TAR_GZ_EXT = Constants.METADATA_TAR_GZ_FILE_EXT;

  private SegmentDeletionManager newManagerWithLocalFs(File dataDir, boolean preserveDeletedSegmentMetadata) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Controller.PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY + ".class",
        LocalPinotFS.class.getName());
    PinotFSFactory.init(new PinotConfiguration(properties));
    return new SegmentDeletionManager(dataDir.getAbsolutePath(), makeHelixAdmin(), CLUSTER_NAME, makePropertyStore(),
        7, preserveDeletedSegmentMetadata);
  }

  private File createLiveSegment(File dataDir, String segmentId, boolean withSegment, boolean withSidecar)
      throws Exception {
    File tableDir = new File(dataDir, TABLE_NAME);
    tableDir.mkdirs();
    if (withSegment) {
      createTestFileWithAge(new File(tableDir, segmentId).getAbsolutePath(), 30);
    }
    if (withSidecar) {
      createTestFileWithAge(new File(tableDir, segmentId + METADATA_TAR_GZ_EXT).getAbsolutePath(), 30);
    }
    return tableDir;
  }

  private static List<String> sortedNames(File dir) {
    String[] names = dir.list();
    if (names == null) {
      return List.of();
    }
    Arrays.sort(names);
    return Arrays.asList(names);
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
          result[i] = !failedSegs.contains(segmentId);
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
    File tempDir = Files.createTempDirectory("pinot-test-").toFile();
    tempDir.deleteOnExit();
    FakeDeletionManager deletionManager = new FakeDeletionManager(
        tempDir.getAbsolutePath(), helixAdmin, propertyStore, 7);
    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);

    // Test delete when deleted segments directory does not exists
    deletionManager.removeAgedDeletedSegments(leadControllerManager);

    // Create deleted directory
    String deletedDirectoryPath = tempDir + File.separator + SegmentDeletionManager.DELETED_SEGMENTS;
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
    Assert.assertFalse(dummyDir1.exists());
    Assert.assertFalse(dummyDir2.exists());
    Assert.assertFalse(dummyDir3.exists());

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

    // Sleep 2 seconds to ensure the clock moves beyond file timestamps (retention checks are time-based)
    Thread.sleep(2000L);

    // Check that dummy directories and files are successfully created.
    Assert.assertEquals(dummyDir1.list().length, 3);
    Assert.assertEquals(dummyDir2.list().length, 3);
    Assert.assertEquals(dummyDir3.list().length, 3);

    // Try to remove files with the retention of 1 days.
    deletionManager.removeAgedDeletedSegments(leadControllerManager);

    // Check that only 1 day retention file is remaining (increased timeout for systems under load)
    TestUtils.waitForCondition((aVoid) -> dummyDir1.list().length == 1, 2000, 120000,
        "Unable to delete desired segments from dummyDir1");

    // dummyDir2's files are all aged, so the first run deletes them (asynchronously, on the deletion
    // manager's executor) but leaves the now-empty directory in place. Empty directories are only
    // removed on a subsequent run. Wait until the directory is present AND empty before triggering
    // that next run: this barrier both asserts the "directory survives the first run" behavior and,
    // critically, guarantees the async file deletions have completed. Previously this only waited for
    // dummyDir2.exists() (trivially true from the moment it is created), so the second run below could
    // race ahead of the async deletions, still see files in dummyDir2, skip the empty-directory
    // removal, and never delete the directory -- surfacing as a 120s "dummyDir2 still exists" timeout.
    TestUtils.waitForCondition((aVoid) -> dummyDir2.exists() && dummyDir2.list().length == 0, 2000, 120000,
        "dummyDir2 was not emptied by the first deletion run");

    // Check that deleted file without retention suffix is honoring cluster-wide retention period of 7 days.
    TestUtils.waitForCondition((aVoid) -> dummyDir3.list().length == 1, 2000, 120000,
        "Unable to delete desired segments from dummyDir3");

    // Try to remove the now-empty directory in the next run
    deletionManager.removeAgedDeletedSegments(leadControllerManager);
    TestUtils.waitForCondition((aVoid) -> !dummyDir2.exists(), 2000, 120000,
        "dummyDir2 still exists");
  }

  @Test
  public void testRemoveDeletedSegmentsForFakePinotFS()
      throws URISyntaxException, IOException, InterruptedException {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Controller.PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY + ".class",
        LocalPinotFS.class.getName());
    PinotFSFactory.init(new PinotConfiguration(properties));

    HelixAdmin helixAdmin = makeHelixAdmin();
    ZkHelixPropertyStore<ZNRecord> propertyStore = makePropertyStore();
    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);

    FakeDeletionManager deletionManager1 = new FakeDeletionManager(
        "fake://bucket/sc/managed/pinot", helixAdmin, propertyStore, 7);

    PinotFSFactory.register("fake", FakePinotFs.class.getName(), null);
    PinotFS pinotFS = PinotFSFactory.create("fake");

    URI tableUri1 = new URI("fake://bucket/sc/managed/pinot/" + SegmentDeletionManager.DELETED_SEGMENTS + "/table_1/");
    pinotFS.mkdir(tableUri1);
    for (int i = 0; i < SegmentDeletionManager.NUM_AGED_SEGMENTS_TO_DELETE_PER_ATTEMPT + 1; i++) {
      URI segmentURIForTable =
          new URI(tableUri1.getPath() + "segment" + i + RETENTION_UNTIL_SEPARATOR + "201901010000");
      pinotFS.mkdir(segmentURIForTable);
    }
    // Add a segment that will not be deleted as it won't meet the retention age criteria.
    URI segmentURIForTable = new URI(tableUri1.getPath() + "segment2" + RETENTION_UNTIL_SEPARATOR + "210001010000");
    pinotFS.mkdir(segmentURIForTable);

    // Create dummy files
    URI tableUri2 = new URI("fake://bucket/sc/managed/pinot/" + SegmentDeletionManager.DELETED_SEGMENTS + "/table_2/");
    URI segment1ForTable2 = new URI(tableUri2.getPath() + "segment1" + RETENTION_UNTIL_SEPARATOR + "201901010000");
    URI segment2ForTable2 = new URI(tableUri2.getPath() + "segment1" + RETENTION_UNTIL_SEPARATOR + "201801010000");
    pinotFS.mkdir(tableUri2);
    pinotFS.mkdir(segment1ForTable2);
    pinotFS.mkdir(segment2ForTable2);

    deletionManager1.removeAgedDeletedSegments(leadControllerManager);

    // All files should get deleted but the directory will be deleted in the next run
    TestUtils.waitForCondition((aVoid) -> {
      try {
        return pinotFS.listFiles(tableUri2, false).length == 0;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 1000, 10000,
        "Could not delete all the files for table_2");
    Assert.assertTrue(pinotFS.exists(tableUri2));

    // One file that doesn't meet retention criteria, and another file due to the per attempt batch limit remains.
    TestUtils.waitForCondition((aVoid) -> {
      try {
        return pinotFS.listFiles(tableUri1, false).length == 2;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 1000, 10000,
        "100 out of 102 files could not be deleted from tableUri1 directory");

    // the next run of the deletion manager should remove the empty directory as well.
    deletionManager1.removeAgedDeletedSegments(leadControllerManager);
    Assert.assertFalse(pinotFS.exists(tableUri2));
  }

  @Test
  public void testSweeperContinuesAfterTableFailure()
      throws Exception {
    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    FakeDeletionManager deletionManager =
        new FakeDeletionManager("fake://bucket/sc/managed/pinot", makeHelixAdmin(), makePropertyStore(), 7);
    PinotFSFactory.register("fake", FakePinotFs.class.getName(), null);
    PinotFS pinotFS = PinotFSFactory.create("fake");

    // Visited first: listing it fails.
    URI failingTable = new URI("fake://bucket/sc/managed/pinot/" + SegmentDeletionManager.DELETED_SEGMENTS
        + "/failingTable/");
    pinotFS.mkdir(failingTable);
    FakePinotFs.FAILING_LISTINGS.add(failingTable.getPath());
    // Visited second: its expired file must still be deleted.
    URI healthyTable = new URI("fake://bucket/sc/managed/pinot/" + SegmentDeletionManager.DELETED_SEGMENTS
        + "/healthyTable/");
    pinotFS.mkdir(healthyTable);
    pinotFS.mkdir(new URI(healthyTable.getPath() + "segment1" + RETENTION_UNTIL_SEPARATOR + "201901010000"));

    deletionManager.removeAgedDeletedSegments(leadControllerManager);

    TestUtils.waitForCondition(aVoid -> {
      try {
        return pinotFS.listFiles(healthyTable, false).length == 0;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 100L, 10_000L, "A failure on one table must not skip the sweep of the next table");
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
    File tempDir = Files.createTempDirectory("pinot-test-").toFile();
    tempDir.deleteOnExit();
    SegmentDeletionManager deletionManager = createDeletionManager(
        tempDir.getAbsolutePath(), helixAdmin, CLUSTER_NAME, propertyStore, 7);

    // create table segment files.
    Set<String> segments = new HashSet<>(segmentsThatShouldBeDeleted());
    createTableAndSegmentFiles(tempDir, segmentsThatShouldBeDeleted());
    final File tableDir = new File(tempDir.getAbsolutePath() + File.separator + TABLE_NAME);
    final File deletedTableDir =
        new File(tempDir.getAbsolutePath() + File.separator + SegmentDeletionManager.DELETED_SEGMENTS
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

  @Test
  public void testSegmentDeletionLogicWithFileWithGZExtension()
      throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Controller.PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY + ".class",
        LocalPinotFS.class.getName());
    PinotFSFactory.init(new PinotConfiguration(properties));

    HelixAdmin helixAdmin = makeHelixAdmin();
    ZkHelixPropertyStore<ZNRecord> propertyStore = makePropertyStore();
    File tempDir = Files.createTempDirectory("pinot-test-").toFile();
    tempDir.deleteOnExit();
    SegmentDeletionManager deletionManager = createDeletionManager(
        tempDir.getAbsolutePath(), helixAdmin, CLUSTER_NAME, propertyStore, 7);

    // create table segment files.
    Set<String> segments = new HashSet<>(segmentsThatShouldBeDeleted());
    createTableAndSegmentFilesWithGZExtension(tempDir, segmentsThatShouldBeDeleted());
    final File tableDir = new File(tempDir.getAbsolutePath() + File.separator + TABLE_NAME);
    final File deletedTableDir =
        new File(tempDir.getAbsolutePath() + File.separator + SegmentDeletionManager.DELETED_SEGMENTS
            + File.separator + TABLE_NAME);

    // mock returning ZK Metadata for segment url
    ZNRecord znRecord1 = mock(org.apache.helix.ZNRecord.class);
    ZNRecord znRecord2 = mock(org.apache.helix.ZNRecord.class);
    ZNRecord znRecord3 = mock(org.apache.helix.ZNRecord.class);
    List<ZNRecord> znRecordList = List.of(znRecord1, znRecord2, znRecord3);
    for (int i = 0; i < 3; i++) {
      when(znRecordList.get(i).getSimpleFields()).thenReturn(Map.of(CommonConstants.Segment.DOWNLOAD_URL,
          tableDir.getAbsolutePath() + File.separator + segmentsThatShouldBeDeleted().get(i)
              + TarCompressionUtils.TAR_GZ_FILE_EXTENSION));
      when(propertyStore.get(constructPropertyStorePathForSegment(TABLE_NAME, segmentsThatShouldBeDeleted().get(i)),
          null, AccessOption.PERSISTENT)).thenReturn(znRecordList.get(i));
    }

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
    createTableAndSegmentFilesWithGZExtension(tempDir, segmentsThatShouldBeDeleted());
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

  @Test
  public void testRemoveSegmentsFromStoreInBatch()
      throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Controller.PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY + ".class",
        LocalPinotFS.class.getName());
    PinotFSFactory.init(new PinotConfiguration(properties));

    HelixAdmin helixAdmin = makeHelixAdmin();
    ZkHelixPropertyStore<ZNRecord> propertyStore = makePropertyStore();
    File tempDir = Files.createTempDirectory("pinot-test-").toFile();
    tempDir.deleteOnExit();
    SegmentDeletionManager deletionManager = createDeletionManager(
        tempDir.getAbsolutePath(), helixAdmin, CLUSTER_NAME, propertyStore, 7);

    // create table segment files.
    List<String> segmentsThatShouldBeDeleted = segmentsThatShouldBeDeleted();
    createTableAndSegmentFiles(tempDir, segmentsThatShouldBeDeleted);
    final File tableDir = new File(tempDir.getAbsolutePath() + File.separator + TABLE_NAME);
    final File deletedTableDir =
        new File(tempDir.getAbsolutePath() + File.separator + SegmentDeletionManager.DELETED_SEGMENTS
            + File.separator + TABLE_NAME);

    deletionManager.removeSegmentsFromStoreInBatch(TABLE_NAME, segmentsThatShouldBeDeleted, 0L);

    TestUtils.waitForCondition(aVoid -> {
      try {
        Assert.assertEquals(tableDir.listFiles().length, 0);
        Assert.assertTrue(!deletedTableDir.exists() || deletedTableDir.listFiles().length == 0);
        return true;
      } catch (Throwable t) {
        return false;
      }
    }, 2000L, 10_000L, "Unable to verify table deletion with retention");
  }

  @Test
  public void testSidecarMovedToMetadataDirWithMatchingSuffix()
      throws Exception {
    File dataDir = Files.createTempDirectory("pinot-test-").toFile();
    dataDir.deleteOnExit();
    SegmentDeletionManager manager = newManagerWithLocalFs(dataDir, true);
    File tableDir = createLiveSegment(dataDir, "seg1", true, true);
    File deletedTableDir = new File(dataDir, SegmentDeletionManager.DELETED_SEGMENTS + File.separator + TABLE_NAME);

    manager.removeSegmentsFromStoreInBatch(TABLE_NAME, Collections.singletonList("seg1"), TimeUnit.DAYS.toMillis(1));

    Assert.assertEquals(tableDir.list().length, 0);
    List<String> names = sortedNames(deletedTableDir);
    // sorted: "metadata" < "seg1__..."
    Assert.assertEquals(names.size(), 2);
    Assert.assertEquals(names.get(0), SegmentDeletionManager.DELETED_SEGMENT_METADATA_DIR);
    String segment = names.get(1);
    Assert.assertTrue(segment.startsWith("seg1" + RETENTION_UNTIL_SEPARATOR), segment);
    Assert.assertEquals(StringUtils.substringAfter(segment, RETENTION_UNTIL_SEPARATOR).length(),
        RETENTION_DATE_FORMAT_STR.length());
    // Same name, so the sidecar carries the segment's retention suffix and expires at the same time.
    Assert.assertEquals(
        sortedNames(new File(deletedTableDir, SegmentDeletionManager.DELETED_SEGMENT_METADATA_DIR)), List.of(segment));
  }

  @Test
  public void testSidecarDeletedWhenRetentionIsZero()
      throws Exception {
    File dataDir = Files.createTempDirectory("pinot-test-").toFile();
    dataDir.deleteOnExit();
    SegmentDeletionManager manager = newManagerWithLocalFs(dataDir, true);
    File tableDir = createLiveSegment(dataDir, "seg1", true, true);
    File deletedTableDir = new File(dataDir, SegmentDeletionManager.DELETED_SEGMENTS + File.separator + TABLE_NAME);

    manager.removeSegmentsFromStoreInBatch(TABLE_NAME, Collections.singletonList("seg1"), 0L);

    Assert.assertEquals(tableDir.list().length, 0);
    Assert.assertTrue(!deletedTableDir.exists() || deletedTableDir.list().length == 0);
  }

  @Test
  public void testSidecarDeletedWhenPreservationDisabled()
      throws Exception {
    File dataDir = Files.createTempDirectory("pinot-test-").toFile();
    dataDir.deleteOnExit();
    SegmentDeletionManager manager = newManagerWithLocalFs(dataDir, false);
    File tableDir = createLiveSegment(dataDir, "seg1", true, true);
    File deletedTableDir = new File(dataDir, SegmentDeletionManager.DELETED_SEGMENTS + File.separator + TABLE_NAME);

    manager.removeSegmentsFromStoreInBatch(TABLE_NAME, Collections.singletonList("seg1"), TimeUnit.DAYS.toMillis(1));

    Assert.assertEquals(tableDir.list().length, 0);
    List<String> names = sortedNames(deletedTableDir);
    Assert.assertEquals(names.size(), 1);
    Assert.assertTrue(names.get(0).startsWith("seg1" + RETENTION_UNTIL_SEPARATOR), names.get(0));
  }

  @Test
  public void testSidecarMovedWithoutSuffixAndTouchedWhenRetentionNull()
      throws Exception {
    File dataDir = Files.createTempDirectory("pinot-test-").toFile();
    dataDir.deleteOnExit();
    SegmentDeletionManager manager = newManagerWithLocalFs(dataDir, true);
    File tableDir = createLiveSegment(dataDir, "seg1", true, true);
    File deletedTableDir = new File(dataDir, SegmentDeletionManager.DELETED_SEGMENTS + File.separator + TABLE_NAME);

    manager.removeSegmentsFromStoreInBatch(TABLE_NAME, Collections.singletonList("seg1"), null);

    Assert.assertEquals(tableDir.list().length, 0);
    Assert.assertEquals(sortedNames(deletedTableDir),
        Arrays.asList(SegmentDeletionManager.DELETED_SEGMENT_METADATA_DIR, "seg1"));
    File metadataDir = new File(deletedTableDir, SegmentDeletionManager.DELETED_SEGMENT_METADATA_DIR);
    Assert.assertEquals(sortedNames(metadataDir), List.of("seg1"));
    // Files were created 30 days old; touch() must have refreshed them so the default retention applies from now.
    long oneMinuteAgo = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1);
    Assert.assertTrue(new File(deletedTableDir, "seg1").lastModified() > oneMinuteAgo);
    Assert.assertTrue(new File(metadataDir, "seg1").lastModified() > oneMinuteAgo);
  }

  @DataProvider
  public Object[][] deletionOrders() {
    return new Object[][]{{List.of("foo", "foo.metadata")}, {List.of("foo.metadata", "foo")}};
  }

  @Test(dataProvider = "deletionOrders")
  public void testSidecarNeverOverwritesArchivedSegment(List<String> deletionOrder)
      throws Exception {
    File dataDir = Files.createTempDirectory("pinot-test-").toFile();
    dataDir.deleteOnExit();
    SegmentDeletionManager manager = newManagerWithLocalFs(dataDir, true);
    File tableDir = new File(dataDir, TABLE_NAME);
    FileUtils.writeStringToFile(new File(tableDir, "foo"), "segment foo", StandardCharsets.UTF_8);
    FileUtils.writeStringToFile(new File(tableDir, "foo.metadata"), "segment foo.metadata", StandardCharsets.UTF_8);
    FileUtils.writeStringToFile(new File(tableDir, "foo" + METADATA_TAR_GZ_EXT), "sidecar foo", StandardCharsets.UTF_8);

    for (String segment : deletionOrder) {
      // Null retention: no suffix, so foo's sidecar and segment foo.metadata would share an archive name without a
      // separate metadata directory.
      manager.removeSegmentsFromStoreInBatch(TABLE_NAME, List.of(segment), null);
    }

    File deletedTableDir = new File(dataDir, SegmentDeletionManager.DELETED_SEGMENTS + File.separator + TABLE_NAME);
    Assert.assertEquals(FileUtils.readFileToString(new File(deletedTableDir, "foo"), StandardCharsets.UTF_8),
        "segment foo");
    Assert.assertEquals(FileUtils.readFileToString(new File(deletedTableDir, "foo.metadata"), StandardCharsets.UTF_8),
        "segment foo.metadata");
    Assert.assertEquals(FileUtils.readFileToString(
            new File(deletedTableDir, SegmentDeletionManager.DELETED_SEGMENT_METADATA_DIR + File.separator + "foo"),
            StandardCharsets.UTF_8), "sidecar foo");
  }

  @Test
  public void testSegmentWithoutSidecarIsUnchanged()
      throws Exception {
    File dataDir = Files.createTempDirectory("pinot-test-").toFile();
    dataDir.deleteOnExit();
    SegmentDeletionManager manager = newManagerWithLocalFs(dataDir, true);
    createLiveSegment(dataDir, "seg1", true, false);
    File deletedTableDir = new File(dataDir, SegmentDeletionManager.DELETED_SEGMENTS + File.separator + TABLE_NAME);

    manager.removeSegmentsFromStoreInBatch(TABLE_NAME, Collections.singletonList("seg1"), TimeUnit.DAYS.toMillis(1));

    List<String> names = sortedNames(deletedTableDir);
    Assert.assertEquals(names.size(), 1);
    Assert.assertTrue(names.get(0).startsWith("seg1" + RETENTION_UNTIL_SEPARATOR));
  }

  private FakeDeletionManager newSweeperWithLocalFs(File dataDir) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Controller.PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY + ".class",
        LocalPinotFS.class.getName());
    PinotFSFactory.init(new PinotConfiguration(properties));
    return new FakeDeletionManager(dataDir.getAbsolutePath(), makeHelixAdmin(), makePropertyStore(), 7);
  }

  @Test
  public void testSweeperExpiresSidecarsInMetadataDir()
      throws Exception {
    File dataDir = Files.createTempDirectory("pinot-test-").toFile();
    dataDir.deleteOnExit();
    FakeDeletionManager manager = newSweeperWithLocalFs(dataDir);
    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    File tableDir = new File(dataDir, SegmentDeletionManager.DELETED_SEGMENTS + File.separator + "myTable");
    File metadataDir = new File(tableDir, SegmentDeletionManager.DELETED_SEGMENT_METADATA_DIR);
    metadataDir.mkdirs();
    // age 2 days with 1 day retention: expired. age 0 with 1 day retention: still retained.
    String expired = genDeletedSegmentName("seg1", 2, 1);
    String retained = genDeletedSegmentName("seg2", 0, 1);
    createTestFileWithAge(new File(tableDir, expired).getAbsolutePath(), 2);
    createTestFileWithAge(new File(metadataDir, expired).getAbsolutePath(), 2);
    createTestFileWithAge(new File(tableDir, retained).getAbsolutePath(), 0);
    createTestFileWithAge(new File(metadataDir, retained).getAbsolutePath(), 0);

    manager.removeAgedDeletedSegments(leadControllerManager);

    TestUtils.waitForCondition(aVoid -> sortedNames(metadataDir).equals(List.of(retained))
            && sortedNames(tableDir).equals(List.of(SegmentDeletionManager.DELETED_SEGMENT_METADATA_DIR, retained)),
        100L, 10_000L, "Unable to verify aged segment and sidecar expiry");
  }

  @Test
  public void testSweeperRemovesEmptyMetadataDirThenTableDir()
      throws Exception {
    File dataDir = Files.createTempDirectory("pinot-test-").toFile();
    dataDir.deleteOnExit();
    FakeDeletionManager manager = newSweeperWithLocalFs(dataDir);
    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    File tableDir = new File(dataDir, SegmentDeletionManager.DELETED_SEGMENTS + File.separator + "myTable");
    File metadataDir = new File(tableDir, SegmentDeletionManager.DELETED_SEGMENT_METADATA_DIR);
    metadataDir.mkdirs();
    String expired = genDeletedSegmentName("seg1", 2, 1);
    createTestFileWithAge(new File(tableDir, expired).getAbsolutePath(), 2);
    createTestFileWithAge(new File(metadataDir, expired).getAbsolutePath(), 2);

    // First sweep deletes the expired files asynchronously; the folders are only removed once empty.
    manager.removeAgedDeletedSegments(leadControllerManager);
    TestUtils.waitForCondition(aVoid -> metadataDir.exists() && sortedNames(metadataDir).isEmpty()
            && sortedNames(tableDir).equals(List.of(SegmentDeletionManager.DELETED_SEGMENT_METADATA_DIR)),
        100L, 10_000L, "Unable to verify aged file expiry");

    // Second sweep removes the empty metadata folder. The table folder is not deleted while its metadata folder
    // existed at the start of the sweep (S3 listings may omit the metadata/ prefix, so the table folder could look
    // empty while it is not).
    manager.removeAgedDeletedSegments(leadControllerManager);
    Assert.assertFalse(metadataDir.exists());
    Assert.assertTrue(tableDir.exists());

    // Third sweep removes the now empty table folder.
    manager.removeAgedDeletedSegments(leadControllerManager);
    Assert.assertFalse(tableDir.exists());
  }

  @Test
  public void testSweeperTreatsSegmentNamedMetadataAsAgedFile()
      throws Exception {
    File dataDir = Files.createTempDirectory("pinot-test-").toFile();
    dataDir.deleteOnExit();
    FakeDeletionManager manager = newSweeperWithLocalFs(dataDir);
    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    File deletedDir = new File(dataDir, SegmentDeletionManager.DELETED_SEGMENTS);
    // A segment named "metadata" deleted with null retention is archived without a suffix, i.e. as a plain file at the
    // metadata directory's path. 30 days old, so it is expired under the 7 day default retention.
    File tableA = new File(deletedDir, "tableA");
    tableA.mkdirs();
    File segmentNamedMetadata = new File(tableA, SegmentDeletionManager.DELETED_SEGMENT_METADATA_DIR);
    createTestFileWithAge(segmentNamedMetadata.getAbsolutePath(), 30);
    File tableB = new File(deletedDir, "tableB");
    tableB.mkdirs();
    File expired = new File(tableB, genDeletedSegmentName("seg1", 2, 1));
    createTestFileWithAge(expired.getAbsolutePath(), 2);

    manager.removeAgedDeletedSegments(leadControllerManager);

    // Must be swept as an ordinary aged file, without aborting the sweep of the other tables (in any listing order).
    TestUtils.waitForCondition(aVoid -> !segmentNamedMetadata.exists() && !expired.exists(), 100L, 10_000L,
        "Unable to verify that a segment named metadata is swept like any other aged file");
  }

  public void createTableAndSegmentFiles(File tempDir, List<String> segmentIds)
      throws Exception {
    File tableDir = new File(tempDir.getAbsolutePath() + File.separator + TABLE_NAME);
    tableDir.mkdir();
    for (String segmentId : segmentIds) {
      createTestFileWithAge(tableDir.getAbsolutePath() + File.separator + segmentId, 0);
      // Create segment metadata file
      createTestFileWithAge(
          tableDir.getAbsolutePath() + File.separator + segmentId + Constants.METADATA_TAR_GZ_FILE_EXT, 0);
    }
  }

  public void createTableAndSegmentFilesWithGZExtension(File tempDir, List<String> segmentIds)
      throws Exception {
    File tableDir = new File(tempDir.getAbsolutePath() + File.separator + TABLE_NAME);
    tableDir.mkdir();
    for (String segmentId : segmentIds) {
      createTestFileWithAge(
          tableDir.getAbsolutePath() + File.separator + segmentId + TarCompressionUtils.TAR_GZ_FILE_EXTENSION, 0);
      // Create segment metadata file
      createTestFileWithAge(
          tableDir.getAbsolutePath() + File.separator + segmentId + Constants.METADATA_TAR_GZ_FILE_EXT, 0);
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
    protected void deleteSegmentsWithDelay(String tableName, Collection<String> segmentIds,
        @Nullable Long deletedSegmentsRetentionMs, long deletionDelaySeconds) {
      _segmentsToRetry.addAll(segmentIds);
    }

    @Override
    public void removeSegmentsFromStoreInBatch(String tableNameWithType, Collection<String> segments,
        @Nullable Long deletedSegmentsRetentionMs) {
      _segmentsRemovedFromStore.addAll(segments);
    }
  }

  public static class FakePinotFs extends LocalPinotFS {

    // SegmentDeletionManager submits deletions on a background single-threaded executor while the
    // test thread may call exists()/listFiles() concurrently; synchronize all accesses to `_tableDirs`
    // to avoid races/CMEs and ensure cross-thread visibility in this test stub.
    // Insertion ordered, so tests can control the order in which the sweeper visits tables.
    private final Map<String, Set<String>> _tableDirs = new LinkedHashMap<>();
    // Table directories (with trailing /) whose listing fails.
    static final Set<String> FAILING_LISTINGS = ConcurrentHashMap.newKeySet();

    @Override
    public void init(PinotConfiguration configuration) {
      _tableDirs.clear();
      FAILING_LISTINGS.clear();
    }

    @Override
    public boolean mkdir(URI uri)
        throws IOException {
      // create a new table Dir if the path ends with /
      if (uri.getPath().endsWith("/")) {
        synchronized (_tableDirs) {
          _tableDirs.put(uri.getPath(), new HashSet<>());
        }
        return true;
      }
      // add the segment to the table dir
      // we are including / in the table path to replicate responses of GcsPinotFs
      String tableName = uri.getPath().substring(0, uri.getPath().lastIndexOf("/") + 1);
      synchronized (_tableDirs) {
        Set<String> segments = _tableDirs.get(tableName);
        return segments != null && segments.add(uri.getPath());
      }
    }

    @Override
    public boolean delete(URI uri, boolean forceDelete)
        throws IOException {
      // delete the key if it's a table
      synchronized (_tableDirs) {
        if (_tableDirs.containsKey(uri.getPath() + "/")) {
          _tableDirs.remove(uri.getPath() + "/");
          return true;
        }
        // remove the segment
        String tableName = uri.getPath().substring(0, uri.getPath().lastIndexOf("/") + 1);
        Set<String> segments = _tableDirs.get(tableName);
        return segments != null && segments.remove(uri.getPath());
      }
    }

    @Override
    public boolean deleteBatch(List<URI> segmentUris, boolean forceDelete)
        throws IOException {
      // the expectation here is that the batch delete call is only limited to segments.
      URI segmentURI = segmentUris.get(0);
      String tableName = segmentURI.getPath().substring(0, segmentURI.getPath().lastIndexOf("/") + 1);
      synchronized (_tableDirs) {
        if (_tableDirs.containsKey(tableName)) {
          // remove all the segments from the table directory
          Set<String> segments = _tableDirs.get(tableName);
          for (URI segmentUri : segmentUris) {
            if (segments != null) {
              segments.remove(segmentUri.getPath());
            }
          }
          return true;
        }
        // the table does not exist and we return a false;
        return false;
      }
    }

    @Override
    public boolean exists(URI fileUri) {
      String uriPath = fileUri.getPath();
      if (uriPath.endsWith("/")) {
        uriPath = uriPath.substring(0, uriPath.lastIndexOf("/"));
      }
      synchronized (_tableDirs) {
        return uriPath.endsWith(SegmentDeletionManager.DELETED_SEGMENTS) || _tableDirs.containsKey(uriPath + "/");
      }
    }

    @Override
    public String[] listFiles(URI fileUri, boolean recursive)
        throws IOException {
      if (fileUri.getPath().endsWith(SegmentDeletionManager.DELETED_SEGMENTS)) {
        synchronized (_tableDirs) {
          return _tableDirs.keySet().toArray(new String[0]);
        }
      }
      // the call to list segments will come without the delimiter after the table name
      String tableName = fileUri.getPath().endsWith("/") ? fileUri.getPath() : fileUri.getPath() + "/";
      synchronized (_tableDirs) {
        Set<String> segments = _tableDirs.get(tableName);
        if (segments == null) {
          return new String[0];
        }
        return segments.toArray(new String[0]);
      }
    }

    @Override
    public List<FileMetadata> listFilesWithMetadata(URI fileUri, boolean recursive)
        throws IOException {
      if (FAILING_LISTINGS.contains(fileUri.getPath() + "/")) {
        throw new IOException("Simulated listing failure: " + fileUri);
      }
      synchronized (_tableDirs) {
        if (_tableDirs.containsKey(fileUri.getPath() + "/")) {
          Set<String> segments = _tableDirs.get(fileUri.getPath() + "/");
          List<FileMetadata> result = new ArrayList<>(segments.size());
          for (String segmentFilePath : segments) {
            result.add(new FileMetadata.Builder().setFilePath(segmentFilePath).build());
          }
          return result;
        }
        return List.of();
      }
    }

    @Override
    public boolean isDirectory(URI uri) {
      // Like ADLSGen2PinotFS, which throws for a missing path instead of returning false. Unchecked because
      // LocalPinotFS.isDirectory does not declare IOException.
      if (!exists(uri)) {
        throw new UncheckedIOException(new IOException("Path does not exist: " + uri));
      }
      synchronized (_tableDirs) {
        if (_tableDirs.containsKey(uri.getPath() + "/")) {
          return true;
        }
        return uri.getPath().endsWith(SegmentDeletionManager.DELETED_SEGMENTS);
      }
    }
  }
}
