package com.linkedin.thirdeye.datasource.pinot.resultset;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.linkedin.thirdeye.dataframe.DataFrame;
import java.io.IOException;
import java.util.ArrayList;

public class ThirdEyeResultSetDeserializer extends StdDeserializer<ThirdEyeResultSet> {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public ThirdEyeResultSetDeserializer() {
    this(null);
  }

  protected ThirdEyeResultSetDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public ThirdEyeResultSet deserialize(JsonParser parser, DeserializationContext deserializationContext)
      throws IOException {

    ObjectCodec codec = parser.getCodec();
    JsonNode rootNode = codec.readTree(parser);

    JsonNode groupColumnNamesNode = rootNode.get(ThirdEyeResultSetSerializer.GROUP_COLUMN_NAMES_FIELD);
    ArrayList<String> groupColumnNames = OBJECT_MAPPER.treeToValue(groupColumnNamesNode, ArrayList.class);

    JsonNode metricColumnNamesNode = rootNode.get(ThirdEyeResultSetSerializer.METRIC_COLUMN_NAMES_FIELD);
    ArrayList<String> metricColumnNames = OBJECT_MAPPER.treeToValue(metricColumnNamesNode, ArrayList.class);

    JsonNode rowsNode = rootNode.get(ThirdEyeResultSetSerializer.ROWS_FIELD);
    ArrayList<ArrayList<String>> rows = OBJECT_MAPPER.treeToValue(rowsNode, ArrayList.class);

    ThirdEyeResultSetMetaData metaData = new ThirdEyeResultSetMetaData(groupColumnNames, metricColumnNames);

    ArrayList<String> columnNamesWithType = new ArrayList<>(groupColumnNames.size());
    for (String groupColumnName : groupColumnNames) {
      columnNamesWithType.add(groupColumnName + ":STRING");
    }
    columnNamesWithType.addAll(metricColumnNames);
    DataFrame.Builder dfBuilder = DataFrame.builder(columnNamesWithType);
    for (ArrayList<String> row : rows) {
      dfBuilder.append(row.toArray());
    }
    DataFrame dataFrame = dfBuilder.build();

    ThirdEyeDataFrameResultSet resultSet = new ThirdEyeDataFrameResultSet(metaData, dataFrame);
    return resultSet;
  }
}
