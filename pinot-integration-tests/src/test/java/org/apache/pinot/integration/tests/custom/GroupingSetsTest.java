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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * End-to-end cluster integration tests for SQL {@code GROUPING SETS} / {@code ROLLUP} / {@code CUBE} /
 * {@code GROUPING()} / {@code GROUPING_ID()}, run over a real cluster on BOTH query engines: the single-stage engine
 * executes them natively, while the multi-stage engine expands them into a UNION ALL of ordinary aggregates. Validates
 * the full broker/server query path including rolled-up columns returned as NULL.
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class GroupingSetsTest extends CustomDataQueryClusterIntegrationTest {
  private static final String COUNTRY_COLUMN = "country";
  private static final String CITY_COLUMN = "city";
  private static final String SALES_COLUMN = "sales";

  private static final List<Object[]> ROWS = List.of(
      new Object[]{"US", "NY", 10},
      new Object[]{"US", "SF", 20},
      new Object[]{"CA", "TO", 30});

  @Override
  public String getTableName() {
    return "GroupingSetsTest";
  }

  @Override
  protected long getCountStarResult() {
    return ROWS.size();
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(COUNTRY_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(CITY_COLUMN, FieldSpec.DataType.STRING)
        .addMetric(SALES_COLUMN, FieldSpec.DataType.INT)
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("groupingSetsRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(COUNTRY_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(CITY_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(SALES_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null)));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < ROWS.size(); i++) {
        Object[] rowData = ROWS.get(i);
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(COUNTRY_COLUMN, rowData[0]);
        record.put(CITY_COLUMN, rowData[1]);
        record.put(SALES_COLUMN, rowData[2]);
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  /** Normalizes the {@code resultTable.rows} of a response into a set of stable per-row strings for comparison. */
  private static Set<String> resultRowKeys(JsonNode response) {
    assertEquals(response.path("exceptions").size(), 0, response.toPrettyString());
    JsonNode rows = response.get("resultTable").get("rows");
    Set<String> keys = new HashSet<>();
    for (JsonNode row : rows) {
      StringBuilder sb = new StringBuilder();
      for (JsonNode value : row) {
        if (value.isNull()) {
          sb.append("null");
        } else if (value.isNumber()) {
          sb.append(value.asLong());
        } else {
          sb.append(value.asText());
        }
        sb.append('|');
      }
      keys.add(sb.toString());
    }
    return keys;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testRollupWithGrouping(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode response = postQuery("SELECT country, city, SUM(sales), GROUPING(country), GROUPING(city) "
        + "FROM " + getTableName() + " GROUP BY ROLLUP(country, city) LIMIT 100");
    Set<String> expected = new HashSet<>(List.of(
        "US|NY|10|0|0|", "US|SF|20|0|0|", "CA|TO|30|0|0|",
        "US|null|30|0|1|", "CA|null|30|0|1|",
        "null|null|60|1|1|"));
    assertEquals(resultRowKeys(response), expected);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testCube(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode response = postQuery("SELECT country, city, SUM(sales) "
        + "FROM " + getTableName() + " GROUP BY CUBE(country, city) LIMIT 100");
    Set<String> expected = new HashSet<>(List.of(
        "US|NY|10|", "US|SF|20|", "CA|TO|30|",
        "US|null|30|", "CA|null|30|",
        "null|NY|10|", "null|SF|20|", "null|TO|30|",
        "null|null|60|"));
    assertEquals(resultRowKeys(response), expected);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGroupingSets(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode response = postQuery("SELECT country, city, SUM(sales) FROM " + getTableName()
        + " GROUP BY GROUPING SETS ((country, city), (country), ()) LIMIT 100");
    Set<String> expected = new HashSet<>(List.of(
        "US|NY|10|", "US|SF|20|", "CA|TO|30|",
        "US|null|30|", "CA|null|30|",
        "null|null|60|"));
    assertEquals(resultRowKeys(response), expected);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGroupingId(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode response = postQuery("SELECT country, city, SUM(sales), GROUPING_ID(country, city) "
        + "FROM " + getTableName() + " GROUP BY ROLLUP(country, city) LIMIT 100");
    Set<String> expected = new HashSet<>(List.of(
        "US|NY|10|0|", "US|SF|20|0|", "CA|TO|30|0|",
        "US|null|30|1|", "CA|null|30|1|",
        "null|null|60|3|"));
    assertEquals(resultRowKeys(response), expected);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testHavingAndOrderBy(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode response = postQuery("SELECT country, SUM(sales), GROUPING(city) FROM " + getTableName()
        + " GROUP BY ROLLUP(country, city) HAVING GROUPING(city) = 1 AND GROUPING(country) = 0 "
        + "ORDER BY country LIMIT 100");
    Set<String> expected = new HashSet<>(List.of("US|30|1|", "CA|30|1|"));
    assertEquals(resultRowKeys(response), expected);
  }
}
