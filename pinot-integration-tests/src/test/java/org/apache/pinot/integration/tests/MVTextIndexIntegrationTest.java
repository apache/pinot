package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.testng.annotations.Test;


public class MVTextIndexIntegrationTest extends OfflineClusterIntegrationTest {
  private static final String TEXT_COLUMN_NAME = "DivAirports";

  private static final String TEST_TEXT_COLUMN_QUERY =
      "SELECT COUNT(*) FROM mytable GROUP BY arrayLength(valueIn(DivAirports,'DFW','ORD'))";

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    return Collections.singletonList(
        new FieldConfig(TEXT_COLUMN_NAME, FieldConfig.EncodingType.RAW, FieldConfig.IndexType.TEXT, null, null));
  }

  @Test
  public void testTextSearchCountQuery()
      throws Exception {
    JsonNode response = postQuery(TEST_TEXT_COLUMN_QUERY);
  }
}
