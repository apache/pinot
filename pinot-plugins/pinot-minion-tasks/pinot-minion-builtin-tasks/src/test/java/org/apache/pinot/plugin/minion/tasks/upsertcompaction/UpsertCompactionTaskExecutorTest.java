package org.apache.pinot.plugin.minion.tasks.upsertcompaction;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class UpsertCompactionTaskExecutorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "UpsertCompactionTaskExecutorTest");
  private static final File ORIGINAL_SEGMENT_DIR = new File(TEMP_DIR, "originalSegment");
  private static final int NUM_ROWS = 5;
  private static final String TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = "testTable_REALTIME";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String D1 = "d1";
  private static final String TS = "ts";
  private static final String TS_VALUE = "1684343768000";
  private File _originalIndexDir;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TABLE_NAME)
        .setTimeColumnName(TS)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .addDateTime(TS, FieldSpec.DataType.LONG, "EPOCH", "1:MILLISECONDS")
        .addSingleValueDimension(D1, FieldSpec.DataType.INT)
        .build();
    schema.setPrimaryKeyColumns(List.of(D1));

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(TS, TS_VALUE);
      row.putValue(D1, i);
      rows.add(row);
    }
    try (GenericRowRecordReader reader = new GenericRowRecordReader(rows)) {
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
      config.setOutDir(ORIGINAL_SEGMENT_DIR.getPath());
      config.setSegmentName(SEGMENT_NAME);

      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, reader);
      driver.build();
      _originalIndexDir = new File(ORIGINAL_SEGMENT_DIR, SEGMENT_NAME);
    }
  }

  @Test
  public void testGetValidIds()
      throws IOException {
    ImmutableRoaringBitmap validDocIds = ImmutableRoaringBitmap.bitmapOf(1, 2, 3);
    List<String> columns = List.of(TS, D1);

    Set<Integer> validIds = UpsertCompactionTaskExecutor.getValidIds(validDocIds, _originalIndexDir, columns);

    Set<Integer> expectedValidIds = new HashSet<>();
    List<String> values = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      values.add(TS_VALUE);
      values.add(Integer.toString(i));
      expectedValidIds.add(values.hashCode());
      values = new ArrayList<>();
    }

    Assert.assertEquals(validIds, expectedValidIds);
  }

  @Test
  public void testGetServer() {
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    Map<String, Map<String, String>> idealStateSegmentAssignment = idealState.getRecord().getMapFields();
    idealStateSegmentAssignment.put(SEGMENT_NAME, Map.of("server1", "server1"));
    HelixAdmin clusterManagementTool = Mockito.mock(HelixAdmin.class);
    MinionContext minionContext = MinionContext.getInstance();
    Mockito.when(clusterManagementTool.getResourceIdealState(minionContext.getClusterName(), REALTIME_TABLE_NAME))
        .thenReturn(idealState);
    minionContext.setClusterManagementTool(clusterManagementTool);

    String server = UpsertCompactionTaskExecutor.getServer(SEGMENT_NAME, REALTIME_TABLE_NAME);

    Assert.assertEquals(server, "server1");
  }
}
