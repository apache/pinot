package org.apache.pinot.segment.local.segment.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexType;
import org.apache.pinot.segment.local.segment.index.inverted.InvertedIndexType;
import org.apache.pinot.segment.spi.index.EmptyIndexConf;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexDeclaration;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;


public class AbstractSerdeIndexContract {

  protected Schema _schema;
  protected TableConfig _tableConfig;

  protected Schema createSchema()
      throws JsonProcessingException {
    return JsonUtils.stringToObject(""
        + "{\n"
        + "  \"schemaName\": \"transcript\",\n"
        + "  \"dimensionFieldSpecs\": [\n"
        + "    {\n"
        + "      \"name\": \"dimInt\",\n"
        + "      \"dataType\": \"INT\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"dimStr\",\n"
        + "      \"dataType\": \"STRING\"\n"
        + "    }\n"
        + "  ],\n"
        + "  \"metricFieldSpecs\": [\n"
        + "    {\n"
        + "      \"name\": \"metInt\",\n"
        + "      \"dataType\": \"INT\"\n"
        + "    }\n"
        + "  ]\n"
        + "}", Schema.class);
  }

  protected TableConfig createTableConfig()
      throws JsonProcessingException {
    return JsonUtils.stringToObject(""
        + "{\n"
        + "  \"tableName\": \"transcript\"\n,"
        + "  \"segmentsConfig\" : {\n"
        + "    \"replication\" : \"1\",\n"
        + "    \"schemaName\" : \"transcript\"\n"
        + "  },\n"
        + "  \"tableIndexConfig\" : {\n"
        + "  },\n"
        + "  \"tenants\" : {\n"
        + "    \"broker\":\"DefaultTenant\",\n"
        + "    \"server\":\"DefaultTenant\"\n"
        + "  },\n"
        + "  \"tableType\":\"OFFLINE\",\n"
        + "  \"metadata\": {}\n"
        + "}", TableConfig.class);
  }
  protected final TypeReference<List<String>> _stringListTypeRef = new TypeReference<List<String>>() {
  };
  protected final TypeReference<List<FieldConfig>> _fieldConfigListTypeRef = new TypeReference<List<FieldConfig>>() {
  };

  @BeforeMethod
  public void reset()
      throws JsonProcessingException {
    _schema = createSchema();
    _tableConfig = createTableConfig();
  }

  protected <C> IndexDeclaration<C> getActualConfig(String column, IndexType<C, ?, ?> type) {
    Map<String, FieldIndexConfigs> confMap =
        FieldIndexConfigsUtil.createIndexConfigsByColName(_tableConfig, _schema, true);

    return confMap.get(column).getConfig(type);
  }

  protected void addFieldIndexConfig(String config)
      throws JsonProcessingException {
    addFieldIndexConfig(JsonUtils.stringToObject(config, FieldConfig.class));
  }

  protected void cleanFieldConfig() {
    _tableConfig.setFieldConfigList(new ArrayList<>());
  }

  protected void addFieldIndexConfig(FieldConfig config) {
    List<FieldConfig> fieldConfigList = _tableConfig.getFieldConfigList();
    if (fieldConfigList == null) {
      fieldConfigList = new ArrayList<>();
    }
    fieldConfigList.add(config);
    _tableConfig.setFieldConfigList(fieldConfigList);
  }

  protected List<String> parseStringList(String json)
      throws IOException {
    return JsonUtils.stringToObject(json, _stringListTypeRef);
  }
}
