package org.apache.pinot.integration.tests;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class UpsertTableSegmentUploadIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 2;
  private static final int NUM_SEGMENTS = 12;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers(getNumBrokers());
    startServers(NUM_SERVERS);

    // Start Kafka
    startKafka();

    // Create and upload the schema.
    Schema schema = createSchema();
    addSchema(schema);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);
    // Create and upload the table config
    TableConfig upsertTableConfig = createUpsertTableConfig(avroFiles.get(0), "clientId");
    addTableConfig(upsertTableConfig);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, upsertTableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @Override
  protected String getSchemaFileName() {
    return "upsert_table_test.schema";
  }

  @Override
  protected String getSchemaName() {
    return "upsertSchema";
  }

  @Override
  protected String getAvroTarFileName() {
    return "upsert_test.tar.gz";
  }

  @Override
  protected boolean useLlc() {
    return true;
  }

  protected int getNumBrokers() {
    return NUM_BROKERS;
  }

  @Override
  protected long getCountStarResult() {
    // Only 1 result is expected as the table is an upsert table and all records of the same primary key are versions of
    // the same record.
    return 1;
  }

  @Test
  public void testSegmentAssignment()
      throws Exception {
    Assert.assertEquals(getCurrentCountStarResult(), 1);
    IdealState idealState = HelixHelper.getTableIdealState(_helixManager, "mytable_REALTIME");

    // Verify various ideal state properties
    Set<String> segments = idealState.getPartitionSet();
    Assert.assertEquals(segments.size(), 3);
    Map<String, Integer> segment2PartitionId = new HashMap<>();
    segment2PartitionId.put("mytable_10027_19736_0", 0);

    // Verify that all segments of the same partition are mapped to the same single server.
    Map<Integer, Set<String>> segmentAssignment = new HashMap<>();
    for (String segment : segments) {
      Integer partitionId;
      if (LLCSegmentName.isLowLevelConsumerSegmentName(segment)) {
        partitionId = new LLCSegmentName(segment).getPartitionGroupId();
      } else {
        partitionId = segment2PartitionId.get(segment);
      }
      Assert.assertNotNull(partitionId);
      Set<String> instances = idealState.getInstanceSet(segment);
      Assert.assertEquals(1, instances.size());
      if (segmentAssignment.containsKey(partitionId)) {
        Assert.assertEquals(instances, segmentAssignment.get(partitionId));
      } else {
        segmentAssignment.put(partitionId, instances);
      }
    }
  }
}
