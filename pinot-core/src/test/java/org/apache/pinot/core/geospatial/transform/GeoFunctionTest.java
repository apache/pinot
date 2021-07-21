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
package org.apache.pinot.core.geospatial.transform;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;


public class GeoFunctionTest {
  protected static final String STRING_SV_COLUMN = "stringSV";
  protected static final String LONG_SV_COLUMN = "longSV";
  protected static final String STRING_SV_COLUMN2 = "stringSV2";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String INDEX_DIR_PATH = FileUtils.getTempDirectoryPath() + File.separator + SEGMENT_NAME;
  protected static final String TIME_COLUMN = "time";

  private static final double DELTA = 0.00001;

  static class Column {
    String name;
    FieldSpec.DataType dataType;
    Object[] values;

    Column(String name, FieldSpec.DataType dataType, Object[] values) {
      this.name = name;
      this.dataType = dataType;
      this.values = values;
    }
  }

  protected void assertRelation(String functionName, String leftWkt, String rightWkt, boolean result)
      throws Exception {
    assertIntFunction(
        String.format("%s(ST_GeomFromText(%s),ST_GeomFromText(%s))", functionName, STRING_SV_COLUMN, STRING_SV_COLUMN2),
        new int[]{result ? 1 : 0}, Arrays
            .asList(new Column(STRING_SV_COLUMN, FieldSpec.DataType.STRING, new String[]{leftWkt}),
                new Column(STRING_SV_COLUMN2, FieldSpec.DataType.STRING, new String[]{rightWkt})));
  }

  protected void assertLongFunction(String function, long[] expectedValues, List<Column> columns)
      throws Exception {
    assertFunction(function, expectedValues.length, columns,
        (TransformFunction transformFunction, ProjectionBlock projectionBlock) -> {
          long[] actualValues = transformFunction.transformToLongValuesSV(projectionBlock);
          for (int i = 0; i < expectedValues.length; i++) {
            Assert.assertEquals(actualValues[i], expectedValues[i]);
          }
        });
  }

  protected void assertIntFunction(String function, int[] expectedValues, List<Column> columns)
      throws Exception {
    assertFunction(function, expectedValues.length, columns,
        (TransformFunction transformFunction, ProjectionBlock projectionBlock) -> {
          int[] actualValues = transformFunction.transformToIntValuesSV(projectionBlock);
          for (int i = 0; i < expectedValues.length; i++) {
            Assert.assertEquals(actualValues[i], expectedValues[i]);
          }
        });
  }

  protected void assertStringFunction(String function, String[] expectedValues, List<Column> columns)
      throws Exception {
    assertFunction(function, expectedValues.length, columns,
        (TransformFunction transformFunction, ProjectionBlock projectionBlock) -> {
          String[] actualValues = transformFunction.transformToStringValuesSV(projectionBlock);
          for (int i = 0; i < expectedValues.length; i++) {
            Assert.assertEquals(actualValues[i], expectedValues[i]);
          }
        });
  }

  protected void assertDoubleFunction(String function, double[] expectedValues, List<Column> columns)
      throws Exception {
    assertFunction(function, expectedValues.length, columns,
        (TransformFunction transformFunction, ProjectionBlock projectionBlock) -> {
          double[] actualValues = transformFunction.transformToDoubleValuesSV(projectionBlock);
          for (int i = 0; i < expectedValues.length; i++) {
            Assert.assertEquals(actualValues[i], expectedValues[i], DELTA);
          }
        });
  }

  private void assertFunction(String function, int length, List<Column> columns,
      BiConsumer<TransformFunction, ProjectionBlock> evaluator)
      throws Exception {
    FileUtils.deleteQuietly(new File(INDEX_DIR_PATH));
    List<GenericRow> rows = new ArrayList<>(length);
    Schema.SchemaBuilder sb = new Schema.SchemaBuilder();
    for (Column column : columns) {
      sb.addSingleValueDimension(column.name, column.dataType);
    }
    Schema schema = sb.build();
    for (int i = 0; i < length; i++) {

      Map<String, Object> map = new HashMap<>();
      for (Column column : columns) {
        map.put(column.name, column.values[i]);
      }
      GenericRow row = new GenericRow();
      row.init(map);
      rows.add(row);
    }

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("test").setTimeColumnName(TIME_COLUMN).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR_PATH);
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    IndexSegment indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR_PATH, SEGMENT_NAME), ReadMode.heap);
    Set<String> columnNames = indexSegment.getPhysicalColumnNames();
    Map<String, DataSource> dataSourceMap = new HashMap<>(columnNames.size());
    for (String columnName : columnNames) {
      dataSourceMap.put(columnName, indexSegment.getDataSource(columnName));
    }

    ProjectionBlock projectionBlock = new ProjectionOperator(dataSourceMap,
        new DocIdSetOperator(new MatchAllFilterOperator(length), DocIdSetPlanNode.MAX_DOC_PER_CALL)).nextBlock();

    ExpressionContext expression = RequestContextUtils.getExpressionFromSQL(function);
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, dataSourceMap);
    evaluator.accept(transformFunction, projectionBlock);
  }
}
