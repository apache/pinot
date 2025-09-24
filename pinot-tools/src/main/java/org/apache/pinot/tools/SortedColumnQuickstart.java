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
package org.apache.pinot.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.tools.admin.PinotAdministrator;

/// Notice: In order to run this quickstart, you need to run the SortedTable main method once to
/// generate the content in the pinot-tools/target/classes/examples/batch/sorted folder.
public class SortedColumnQuickstart extends Quickstart {
  private static final String QUICKSTART_IDENTIFIER = "SORTED";
  @Override
  public List<String> types() {
    return Collections.singletonList(QUICKSTART_IDENTIFIER);
  }

  @Override
  protected String[] getDefaultBatchTableDirectories() {
    // contrary to other quickstarts, here we create the content automatically.
    // it is important to notice this path is not a file path but a resource path on the classpath.
    // this means we can create the content in the pinot-tools/target/classes/examples/batch/sorted folder
    return new String[] { "examples/batch/sorted" };
  }

  @Override
  protected Map<String, String> getDefaultStreamTableDirectories() {
    return Map.of();
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", QUICKSTART_IDENTIFIER));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }

  public static class SortedTable {
    private static final String TABLE_NAME = "sorted";
    private static final String INT_COL_NAME = "INT_COL";
    private static final String SORTED_COL_NAME = "SORTED_COL";
    private static final String RAW_INT_COL_NAME = "RAW_INT_COL";
    private static final String RAW_STRING_COL_NAME = "RAW_STRING_COL";
    private static final String NO_INDEX_INT_COL_NAME = "NO_INDEX_INT_COL";
    private static final String NO_INDEX_STRING_COL = "NO_INDEX_STRING_COL";
    private static final String LOW_CARDINALITY_STRING_COL = "LOW_CARDINALITY_STRING_COL";
    private static final List<FieldConfig> FIELD_CONFIGS = new ArrayList<>();

    public static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(List.of(INT_COL_NAME, LOW_CARDINALITY_STRING_COL))
        .setFieldConfigList(FIELD_CONFIGS)
        .setNoDictionaryColumns(List.of(RAW_INT_COL_NAME, RAW_STRING_COL_NAME))
        .setSortedColumn(SORTED_COL_NAME)
        .setRangeIndexColumns(List.of(INT_COL_NAME, LOW_CARDINALITY_STRING_COL))
        .setStarTreeIndexConfigs(Collections.singletonList(
            new StarTreeIndexConfig(Arrays.asList(SORTED_COL_NAME, INT_COL_NAME), null, Collections.singletonList(
                new AggregationFunctionColumnPair(AggregationFunctionType.SUM, RAW_INT_COL_NAME).toColumnName()), null,
                Integer.MAX_VALUE)))
        .build();
    public static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(SORTED_COL_NAME, FieldSpec.DataType.INT)
        .addSingleValueDimension(NO_INDEX_INT_COL_NAME, FieldSpec.DataType.INT)
        .addSingleValueDimension(RAW_INT_COL_NAME, FieldSpec.DataType.INT)
        .addSingleValueDimension(INT_COL_NAME, FieldSpec.DataType.INT)
        .addSingleValueDimension(RAW_STRING_COL_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(NO_INDEX_STRING_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension(LOW_CARDINALITY_STRING_COL, FieldSpec.DataType.STRING)
        .build();

    public static void main(String[] args)
        throws Exception {
      Path activeDir = new File("").getAbsoluteFile().toPath();
      Path pinotToolsPath;
      if (activeDir.endsWith("pinot")) {
        pinotToolsPath = activeDir.resolve("pinot-tools");
      } else {
        pinotToolsPath = activeDir;
      }
      Path bootstrapDir = pinotToolsPath.resolve(Path.of("src", "main", "resources", "examples", "batch", "sorted"));
      prepareBootstrapDir(bootstrapDir);
    }

    private static void prepareBootstrapDir(Path bootstrapDir)
        throws IOException {
      File bootstrapDirFile = bootstrapDir.toFile();
      bootstrapDirFile.mkdirs();

      ObjectMapper objectMapper = new ObjectMapper();
      File tableConfigFile = bootstrapDir.resolve("sorted_offline_table_config.json").toFile();
      objectMapper.writerWithDefaultPrettyPrinter().writeValue(tableConfigFile, SortedTable.TABLE_CONFIG);
      File schemaFile = bootstrapDir.resolve("sorted_schema.json").toFile();
      objectMapper.writerWithDefaultPrettyPrinter().writeValue(schemaFile, SortedTable.SCHEMA);

      File rawdata = bootstrapDir.resolve("rawdata").toFile();
      rawdata.mkdirs();

      try (FileWriter fw = new FileWriter(new File(rawdata, "data.csv"))) {
        // write the CSV header
        fw.append(String.join(",", SortedTable.SORTED_COL_NAME, SortedTable.INT_COL_NAME,
            SortedTable.NO_INDEX_INT_COL_NAME, SortedTable.RAW_INT_COL_NAME, SortedTable.RAW_STRING_COL_NAME,
            SortedTable.NO_INDEX_STRING_COL, SortedTable.LOW_CARDINALITY_STRING_COL)).append('\n');

        Random r = new Random(42);
        streamData(1000, r::nextLong)
            .limit(100_000)
            .forEach(s -> {
              // generate CSV line
              try {
                fw.append(String.valueOf(s.getValue(SortedTable.SORTED_COL_NAME))).append(',');
                fw.append(String.valueOf(s.getValue(SortedTable.INT_COL_NAME))).append(',');
                fw.append(String.valueOf(s.getValue(SortedTable.NO_INDEX_INT_COL_NAME))).append(',');
                fw.append(String.valueOf(s.getValue(SortedTable.RAW_INT_COL_NAME))).append(',');
                fw.append(String.valueOf(s.getValue(SortedTable.RAW_STRING_COL_NAME))).append(',');
                fw.append(String.valueOf(s.getValue(SortedTable.NO_INDEX_STRING_COL))).append(',');
                fw.append(String.valueOf(s.getValue(SortedTable.LOW_CARDINALITY_STRING_COL))).append('\n');
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
      }
    }

    public static Stream<GenericRow> streamData(int primaryRepetitions, LongSupplier longSupplier) {
      Map<Integer, String> strings = new HashMap<>();
      String[] lowCardinalityValues = IntStream.range(0, 10).mapToObj(i -> "value" + i)
          .toArray(String[]::new);
      return IntStream.iterate(0, i -> i + 1)
          .boxed()
          .flatMap(i -> IntStream.range(0, primaryRepetitions)
              .mapToObj(j -> {
                GenericRow row = new GenericRow();
                row.putValue(SortedTable.SORTED_COL_NAME, i);
                row.putValue(SortedTable.INT_COL_NAME, (int) longSupplier.getAsLong());
                row.putValue(SortedTable.NO_INDEX_INT_COL_NAME, (int) longSupplier.getAsLong());
                row.putValue(SortedTable.RAW_INT_COL_NAME, (int) longSupplier.getAsLong());
                row.putValue(SortedTable.RAW_STRING_COL_NAME, strings.computeIfAbsent(
                    (int) longSupplier.getAsLong(), k -> UUID.randomUUID().toString()));
                row.putValue(SortedTable.NO_INDEX_STRING_COL, row.getValue(SortedTable.RAW_STRING_COL_NAME));
                int lowCardinalityIndex = (i + j) % lowCardinalityValues.length;
                row.putValue(SortedTable.LOW_CARDINALITY_STRING_COL, lowCardinalityValues[lowCardinalityIndex]);
                return row;
              })
          );
    }
  }
}
