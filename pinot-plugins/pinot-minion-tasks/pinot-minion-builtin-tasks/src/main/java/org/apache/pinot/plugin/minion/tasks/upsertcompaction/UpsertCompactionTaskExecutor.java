package org.apache.pinot.plugin.minion.tasks.upsertcompaction;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.core.minion.SegmentPurger;
import org.apache.pinot.plugin.minion.tasks.BaseSingleSegmentConversionExecutor;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public class UpsertCompactionTaskExecutor extends BaseSingleSegmentConversionExecutor {
  public static final String RECORD_PURGER_KEY = "recordPurger";
  public static final String NUM_RECORDS_PURGED_KEY = "numRecordsPurged";

  @Override
  protected SegmentConversionResult convert(PinotTaskConfig pinotTaskConfig, File indexDir, File workingDir)
    throws Exception {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    String segmentName = configs.get(MinionConstants.SEGMENT_NAME_KEY);
    String server = getServer(segmentName, tableNameWithType);

    // get the url for the validDocIds for the server
    InstanceConfig instanceConfig =
        MINION_CONTEXT.getClusterManagementTool().getInstanceConfig(MINION_CONTEXT.getClusterName(), server);
    String endpoint = InstanceUtils.getServerAdminEndpoint(instanceConfig);
    String url = String.format("%s/segments/%s/%s/validDocIds",
        endpoint, tableNameWithType, segmentName);

    // get the validDocIds from that server
    Response response = ClientBuilder.newClient().target(url).request().get(Response.class);
    Preconditions.checkState(response.getStatus() == Response.Status.OK.getStatusCode(),
        "Unable to retrieve validDocIds from %s", url);
    byte[] snapshot = response.readEntity(byte[].class);
    ImmutableRoaringBitmap bitmap = new ImmutableRoaringBitmap(ByteBuffer.wrap(snapshot));

    // transform validDocIds into validIds using pk columns and time column
    List<String> columns = getSchema(rawTableName).getPrimaryKeyColumns();
    TableConfig tableConfig = getTableConfig(tableNameWithType);
    columns.add(tableConfig.getValidationConfig().getTimeColumnName());
    PeekableIntIterator iterator = bitmap.getIntIterator();
    PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader();
    recordReader.init(indexDir, new HashSet<>(columns), null);
    GenericRow genericRow = new GenericRow();
    List<String> validIds = new ArrayList<>();
    while (iterator.hasNext()) {
      int validDocId = iterator.next();
      recordReader.getRecord(validDocId, genericRow);
      StringBuilder validId = new StringBuilder();
      for (String column : columns) {
        validId.append(genericRow.getValue(column).toString());
      }
      validIds.add(validId.toString());
    }
    recordReader.close();

    MINION_CONTEXT.setRecordPurgerFactory(x -> row -> {
      StringBuilder id = new StringBuilder();
      for (String column : columns) {
        id.append(row.getValue(column).toString());
      }
      return !validIds.contains(id.toString());
    });
    SegmentPurger.RecordPurger recordPurger = MINION_CONTEXT
        .getRecordPurgerFactory()
        .getRecordPurger(rawTableName);

    SegmentPurger segmentPurger = new SegmentPurger(indexDir, workingDir, tableConfig, recordPurger, null);
    File compactedSegmentFile = segmentPurger.purgeSegment();
    if (compactedSegmentFile == null) {
      compactedSegmentFile = indexDir;
    }

    return new SegmentConversionResult.Builder().setFile(compactedSegmentFile)
        .setTableNameWithType(tableNameWithType)
        .setSegmentName(configs.get(MinionConstants.SEGMENT_NAME_KEY))
        .setCustomProperty(RECORD_PURGER_KEY, segmentPurger.getRecordPurger())
        .setCustomProperty(NUM_RECORDS_PURGED_KEY, segmentPurger.getNumRecordsPurged())
        .build();
  }

  private static String getServer(String segmentName, String tableNameWithType) {
    String server = null;
    HelixAdmin clusterManagementTool = MINION_CONTEXT.getClusterManagementTool();
    IdealState idealState =
        clusterManagementTool.getResourceIdealState(MINION_CONTEXT.getClusterName(), tableNameWithType);
    if (idealState == null) {
      throw new IllegalStateException("Ideal state does not exist for table: "+ tableNameWithType);
    }
    for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
      String segment = entry.getKey();
      if (Objects.equals(segment, segmentName)) {
        server = entry.getValue().keySet().toArray()[0].toString();
        break;
      }
    }
    return server;
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(PinotTaskConfig pinotTaskConfig,
      SegmentConversionResult segmentConversionResult) {
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE,
        Collections.singletonMap(MinionConstants.UpsertCompactionTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX,
            String.valueOf(System.currentTimeMillis())));
  }
}
