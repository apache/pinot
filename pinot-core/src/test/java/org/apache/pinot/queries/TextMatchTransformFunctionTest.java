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
package org.apache.pinot.queries;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TextMatchTransformFunctionTest {

  protected static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE)
          .setTableName("testTable")
          .addFieldConfig(
              new FieldConfig("skills", FieldConfig.EncodingType.RAW,
                  List.of(FieldConfig.IndexType.TEXT), null, null))
          .build();

  protected File _baseDir;

  @BeforeClass
  void createBaseDir() {
    try {
      _baseDir = Files.createTempDirectory(getClass().getSimpleName()).toFile();
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  @AfterClass
  void destroyBaseDir()
      throws IOException {
    if (_baseDir != null) {
      FileUtils.deleteDirectory(_baseDir);
    }
  }

  @Test
  void testTextMatchValidation() {
    try {
      FluentQueryTest.withBaseDir(_baseDir)
          .givenTable(
              new Schema.SchemaBuilder()
                  .setSchemaName("testTable")
                  .addMetricField("id", FieldSpec.DataType.INT)
                  .addSingleValueDimension("skills", FieldSpec.DataType.STRING)
                  .build(),
              TABLE_CONFIG)
          .onFirstInstance(
              new Object[]{1, "swimming"}
          )
          .whenQuery("select TEXT_MATCH(id, 'sewing') as match from testTable");
      Assert.fail();
    } catch (BadQueryRequestException e) {
      Assert.assertEquals(e.getCause().getMessage(), "Cannot apply TEXT_MATCH on column: id without text index");
    }

    try {
      FluentQueryTest.withBaseDir(_baseDir)
          .givenTable(
              new Schema.SchemaBuilder()
                  .setSchemaName("testTable")
                  .addMetricField("id", FieldSpec.DataType.INT)
                  .addSingleValueDimension("skills", FieldSpec.DataType.STRING)
                  .build(),
              new TableConfigBuilder(TableType.OFFLINE)
                  .setTableName("testTable")
                  .addFieldConfig(
                      new FieldConfig("skills", FieldConfig.EncodingType.RAW,
                          List.of(FieldConfig.IndexType.TEXT), null,
                          Map.of(FieldConfig.TEXT_FST_TYPE, FieldConfig.TEXT_NATIVE_FST_LITERAL)))
                  .build())
          .onFirstInstance(
              new Object[]{1, "swimming"}
          )
          .whenQuery("select TEXT_MATCH(skills, 'sewing') as match from testTable");
      Assert.fail();
    } catch (BadQueryRequestException e) {
      Assert.assertEquals(e.getCause().getMessage(),
          "TEXT_MATCH is not supported on column: skills with native text index");
    }

    try {
      FluentQueryTest.withBaseDir(_baseDir)
          .givenTable(
              new Schema.SchemaBuilder()
                  .setSchemaName("testTable")
                  .addMetricField("id", FieldSpec.DataType.INT)
                  .addSingleValueDimension("skills", FieldSpec.DataType.STRING)
                  .build(),
              TABLE_CONFIG)
          .onFirstInstance(
              new Object[]{1, "sewing, cooking"}
          )
          .whenQuery("select TEXT_MATCH('id', 'sewing') as match from testTable");
      Assert.fail();
    } catch (BadQueryRequestException e) {
      Assert.assertEquals(e.getCause().getMessage(),
          "The first argument of TEXT_MATCH transform function must be a single-valued column");
    }

    try {
      FluentQueryTest.withBaseDir(_baseDir)
          .givenTable(
              new Schema.SchemaBuilder()
                  .setSchemaName("testTable")
                  .addMultiValueDimension("id", FieldSpec.DataType.STRING)
                  .addSingleValueDimension("skills", FieldSpec.DataType.STRING)
                  .build(),
              TABLE_CONFIG)
          .onFirstInstance(
              new Object[]{1, "sewing, cooking"}
          )
          .whenQuery("select TEXT_MATCH(id, 'sewing') as match from testTable");
      Assert.fail();
    } catch (BadQueryRequestException e) {
      Assert.assertEquals(e.getCause().getMessage(),
          "The first argument of TEXT_MATCH transform function must be a single-valued column");
    }

    try {
      FluentQueryTest.withBaseDir(_baseDir)
          .givenTable(
              new Schema.SchemaBuilder()
                  .setSchemaName("testTable")
                  .addMultiValueDimension("id", FieldSpec.DataType.STRING)
                  .addSingleValueDimension("skills", FieldSpec.DataType.STRING)
                  .build(),
              TABLE_CONFIG)
          .onFirstInstance(
              new Object[]{1, "sewing, cooking"}
          )
          .whenQuery("select TEXT_MATCH(skills, id) as match from testTable");
      Assert.fail();
    } catch (BadQueryRequestException e) {
      Assert.assertEquals(e.getCause().getMessage(),
          "The second argument of TEXT_MATCH transform function must be a single-valued string literal");
    }
  }

  @Test
  void testTextMatchAsTransformFunction() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMetricField("id", FieldSpec.DataType.INT)
                .addSingleValueDimension("skills", FieldSpec.DataType.STRING)
                .build(),
            TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1, "sewing, cooking"},
            new Object[]{2, "washing, cleaning"}
        )
        .andOnSecondInstance(
            new Object[]{3, "skiing, running"},
            new Object[]{4, "singing, sewing"}
        )
        .whenQuery("select id, skills, TEXT_MATCH(skills, 'sewing') as match from testTable order by id limit 100000")
        .thenResultTextIs(
            "id[INT] | skills[STRING] | match[BOOLEAN]\n"
                + "1 | sewing, cooking | true\n"
                + "2 | washing, cleaning | false\n"
                + "3 | skiing, running | false\n"
                + "4 | singing, sewing | true"
        ).whenQuery(
            "select id, skills, case when skills = 'AAA' then '?' "
                + "when  TEXT_MATCH(skills, 'sewing') then 'ok' else 'wrong' end as status "
                + "from testTable "
                + "order by id "
                + "limit 100000")
        .thenResultTextIs(
            "id[INT] | skills[STRING] | status[STRING]\n"
                + "1 | sewing, cooking | ok\n"
                + "2 | washing, cleaning | wrong\n"
                + "3 | skiing, running | wrong\n"
                + "4 | singing, sewing | ok"
        ).whenQuery(
            "select id, skills "
                + "from testTable "
                + "order by TEXT_MATCH(skills, 'sewing'), id "
                + "limit 100000")
        .thenResultTextIs(
            "id[INT] | skills[STRING]\n"
                + "2 | washing, cleaning\n"
                + "3 | skiing, running\n"
                + "1 | sewing, cooking\n"
                + "4 | singing, sewing"
        );
  }

  @Test
  public void testTextMatchInAggregation() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMetricField("id", FieldSpec.DataType.INT)
                .addSingleValueDimension("skills", FieldSpec.DataType.STRING)
                .build(),
            TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1, "sewing, cooking"},
            new Object[]{2, "washing, cleaning"}
        )
        .andOnSecondInstance(
            new Object[]{3, "skiing, running"},
            new Object[]{4, "singing, sewing"}
        )
        .whenQuery("select TEXT_MATCH(skills, 'sewing') as match, count(*) "
            + "from testTable "
            + "group by 1 "
            + "order by 1 ")
        .thenResultTextIs(
            "match[BOOLEAN] | count(*)[LONG]\n"
                + "false | 2\n"
                + "true | 2"
        ).whenQuery("select TEXT_MATCH(skills, 'sewing') as match, count(*) "
            + "from testTable "
            + "group by 1 "
            + "order by 1 ")
        .thenResultTextIs(
            "match[BOOLEAN] | count(*)[LONG]\n"
                + "false | 2\n"
                + "true | 2"
        );
  }

  @Test
  public void testExplainTextMatch() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMetricField("id", FieldSpec.DataType.INT)
                .addSingleValueDimension("skills", FieldSpec.DataType.STRING)
                .build(),
            TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{1, "sewing, cooking"}
        )
        .whenQuery("explain plan for select TEXT_MATCH(skills, 'sewing') as match "
            + "from testTable "
            + "where TEXT_MATCH(skills, 'swimming')")
        .thenResultTextIs(
            "Operator[STRING] | Operator_Id[INT] | Parent_Id[INT]\n"
                + "BROKER_REDUCE(limit:10) | 1 | 0\n"
                + "COMBINE_SELECT | 2 | 1\n"
                + "PLAN_START(numSegmentsForThisPlan:2) | -1 | -1\n"
                + "SELECT(selectList:text_match(skills,'sewing')) | 3 | 2\n"
                + "TRANSFORM(text_match(skills,'sewing')) | 4 | 3\n"
                + "PROJECT(skills) | 5 | 4\n"
                + "DOC_ID_SET | 6 | 5\n"
                + "FILTER_TEXT_INDEX(indexLookUp:text_index,operator:TEXT_MATCH,predicate:text_match(skills,"
                + "'swimming')) | 7 | 6"
        );
  }

  @Test
  void testTextMatchComplexQuery() {
    String query =
        "SELECT "
            + " ( "
            + "   case "
            + "     when agent is null then 'N/A' "
            + "     when TEXT_MATCH(part, '_zz_') AND part IS NOT NULL then agent "
            + "     else '' "
            + "   end "
            + " ) as val "
            + "FROM testTable "
            + " WHERE startTime BETWEEN 0 AND 1000000 "
            + "  AND customerId = 'XYZ'  "
            + "ORDER BY startTime ASC "
            + "LIMIT 1000";

    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addDimensionField("agent", FieldSpec.DataType.STRING, f -> {
                  f.setNullable(true);
                  f.setSingleValueField(true);
                })
                .addSingleValueDimension("customerId", FieldSpec.DataType.STRING)
                .addSingleValueDimension("part", FieldSpec.DataType.STRING)
                .addDateTime("startTime", FieldSpec.DataType.TIMESTAMP, "TIMESTAMP", "1:MILLISECONDS")
                .build(),
            new TableConfigBuilder(TableType.OFFLINE)
                .setTableName("testTable")
                .addFieldConfig(new FieldConfig("agent", FieldConfig.EncodingType.DICTIONARY,
                    List.of(), null, null))
                .addFieldConfig(
                    new FieldConfig("customerId", FieldConfig.EncodingType.DICTIONARY,
                        List.of(FieldConfig.IndexType.INVERTED), null, null))
                .addFieldConfig(
                    new FieldConfig("part", FieldConfig.EncodingType.RAW,
                        List.of(FieldConfig.IndexType.TEXT), null, null))
                .addFieldConfig(
                    new FieldConfig("startTime", FieldConfig.EncodingType.RAW,
                        List.of(FieldConfig.IndexType.RANGE), null, null))
                .build())
        .onFirstInstance(
            new Object[]{null, "XYZ", "_zz_", 1L},
            new Object[]{"A1", "XYZ", "_zz_", 20L}
        )
        .andOnSecondInstance(
            new Object[]{"A2", "XYZ", "_zz_", 1000L},
            new Object[]{"A3", "DEF", "_zz_", 2000L}
        )
        .whenQuery(
            "set explainAskingServers=true; "
                + "EXPLAIN PLAN FOR " + query
        )
        .thenResultTextIs(
            "Operator[STRING] | Operator_Id[INT] | Parent_Id[INT]\n"
                + "BROKER_REDUCE(sort:[startTime ASC],limit:1000) | 1 | 0\n"
                + "COMBINE_SELECT_ORDERBY_MINMAX | 2 | 1\n"
                + "PLAN_START(numSegmentsForThisPlan:1) | -1 | -1\n"
                + "SELECT_PARTIAL_ORDER_BY_ASC(sortedList: (startTime), unsortedList: (), rest: (case(is_null(agent),"
                + "'N/A',and(text_match(part,'_zz_'),is_not_null(part)),agent,''))) | 3 | 2\n"
                + "TRANSFORM(case(is_null(agent),'N/A',and(text_match(part,'_zz_'),is_not_null(part)),agent,''), "
                + "startTime) | 4 | 3\n"
                + "PROJECT(agent, startTime, part) | 5 | 4\n"
                + "DOC_ID_SET | 6 | 5\n"
                + "FILTER_AND | 7 | 6\n"
                + "FILTER_FULL_SCAN(operator:RANGE,predicate:startTime BETWEEN '0' AND '1000000') | 8 | 7\n"
                + "FILTER_FULL_SCAN(operator:EQ,predicate:customerId = 'XYZ') | 9 | 7"
        ).whenQuery(query)
        .thenResultTextIs("val[STRING]\n"
            + "N/A\n"
            + "A1\n"
            + "A2");
  }
}
