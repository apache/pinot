package org.apache.pinot.integration.tests.realtime.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.controller.api.resources.TableViews;
import org.apache.pinot.integration.tests.realtime.ingestion.utils.KinesisUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;

public class KinesisShardChangeTest extends BaseKinesisIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisShardChangeTest.class);

  // TODO - Check with full airlineStats data with new local stack version
  private static final String SCHEMA_FILE_PATH = "kinesis/airlineStats_data_reduced.schema";
  private static final String DATA_FILE_PATH = "kinesis/airlineStats_data_reduced.json";

  @Test
  public void testIncreaseShardsWithPauseAndResume()
      throws Exception {

    createStream(2);
    addSchema(createSchema(SCHEMA_FILE_PATH));
    TableConfig tableConfig = createRealtimeTableConfig(null);
    addTableConfig(tableConfig);

    publishRecordsToKinesis(DATA_FILE_PATH, 0, 50);
    waitForRecordsToBeConsumed(tableConfig.getTableName(), 50);

    KinesisUtils.splitNthShard(_kinesisClient, STREAM_NAME, 0);
    KinesisUtils.splitNthShard(_kinesisClient, STREAM_NAME, 1);

    pauseTable(tableConfig.getTableName());
    publishRecordsToKinesis(DATA_FILE_PATH, 50, 200);

    resumeTable(tableConfig.getTableName(), "smallest");

    waitForRecordsToBeConsumed(tableConfig.getTableName(), 250);

    TableViews.TableView tableView = getExternalView(tableConfig.getTableName());
    Assert.assertEquals(tableView._realtime.size(), 6);

  }

  /**
   * start and end offsets are essentially the start row index and end row index of the file
   * @param startOffset - inclusive
   * @param endOffset - exclusive
   * @return the number of records published to Kinesis
   */
  private int publishRecordsToKinesis(String dataFilePath, int startOffset, int endOffset) throws Exception {
    InputStream inputStream =
        RealtimeKinesisIntegrationTest.class.getClassLoader().getResourceAsStream(dataFilePath);
    int numRecordsPushed = 0;
    try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      String line;
      int count = 0;
      while ((line = br.readLine()) != null) {
        // Skip the first startOffset lines
        if (count < startOffset) {
          count++;
          continue;
        }
        if (count++ >= endOffset) {
          break;
        }
        JsonNode data = JsonUtils.stringToJsonNode(line);
        PutRecordResponse putRecordResponse = putRecord(line, data.get("Origin").textValue());
        if (putRecordResponse.sdkHttpResponse().statusCode() == 200) {
          numRecordsPushed++;
        } else {
          throw new RuntimeException("Failed to put record " + line + " to Kinesis stream with status code: "
              + putRecordResponse.sdkHttpResponse().statusCode());
        }
      }
    }
    return numRecordsPushed;
  }

  private void waitForRecordsToBeConsumed(String tableName, int expectedNumRecords) {
    TestUtils.waitForCondition(aVoid -> {
      try {
        long count = getPinotConnection().execute("SELECT COUNT(*) FROM " + tableName).getResultSet(0).getLong(0);
        LOGGER.warn("Got count: {}", count);
        // todo - how to validate that we're not getting more than expected records. Eg: if we get 250 records but assertion happens at 200 records. should we sleep before ?
        return count == expectedNumRecords;
      } catch (Exception e) {
        return false;
      }
    }, 5 * 60 * 1000L, "Wait for all records to be ingested");
  }

  @Override
  public List<String> getNoDictionaryColumns() {
    return Collections.emptyList();
  }

  @Override
  public String getSortedColumn() {
    return null;
  }

}
