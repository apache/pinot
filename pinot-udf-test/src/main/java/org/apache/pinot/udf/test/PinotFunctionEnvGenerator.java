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
package org.apache.pinot.udf.test;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.util.Preconditions;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExample.NullHandling;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;


/// Class used to generate the [TableConfig] and [Schema] for the Pinot function tests.
public class PinotFunctionEnvGenerator {

  private PinotFunctionEnvGenerator() {
  }

  public static void prepareEnvironment(UdfTestCluster cluster, Iterable<Udf> udfs) {
    Map<String, List<Udf>> udfByTableName = new HashMap<>();
    for (Udf udf : udfs) {
      String tableName = getTableName(udf);
      udfByTableName.computeIfAbsent(tableName, k -> new ArrayList<>()).add(udf);
    }

    for (Map.Entry<String, List<Udf>> tableEntry : udfByTableName.entrySet()) {
      String tableName = tableEntry.getKey();
      List<Udf> sameTableUdfs = tableEntry.getValue();
      Schema schema = generateSchema(sameTableUdfs);
      cluster.addTable(schema, generateTableConfig(sameTableUdfs));

      cluster.addRows(
          tableName,
          schema,
          sameTableUdfs.stream()
              .flatMap(udf -> udf.getExamples().entrySet().stream()
                  .flatMap(exampleEntry -> {
                    UdfSignature signature = exampleEntry.getKey();
                    return exampleEntry.getValue().stream()
                        .map(testCase -> asRow(udf, signature, testCase));
                  })
              )
      );
    }
  }

  public static String getTableName(Udf udf) {
    if (udf.getExamples().keySet().stream()
        .flatMap(signature -> signature.getParameters().stream())
        .anyMatch(param -> !param.getConstraints().isEmpty())) {
      return "udf_test_" + udf.getMainCanonicalName().replaceAll("[^a-zA-Z0-9]", "_");
    }
    return "udf_test";
  }

  public static String getUdfColumnName() {
    return "udf";
  }

  public static String getTestColumnName() {
    return "test";
  }

  public static String getSignatureColumnName() {
    return "signature";
  }

  public static List<String> getArgsForCall(UdfSignature signature) {
    List<UdfParameter> params = signature.getParameters();
    List<String> args = new ArrayList<>(params.size());
    for (int i = 0; i < params.size(); i++) {
      args.add(getParameterColumnName(signature, i));
    }
    return args;
  }

  public static List<String> getArgsForCall(UdfSignature signature, UdfExample example) {
    List<UdfParameter> params = signature.getParameters();
    List<String> args = new ArrayList<>(params.size());
    for (int i = 0; i < params.size(); i++) {
      UdfParameter param = params.get(i);
      String paramValue;
      if (param.isLiteralOnly()) {
        Object exampleValue = example.getInputValues().get(i);
        if (exampleValue == null) {
          paramValue = "NULL";
        } else if (exampleValue.getClass().isArray()) {
          paramValue = Arrays.toString((Object[]) exampleValue);
        } else if (param.getDataType() == FieldSpec.DataType.STRING) {
          paramValue = "'" + exampleValue.toString().replace("'", "''") + "'";
        } else if (param.getDataType() == FieldSpec.DataType.BYTES) {
          paramValue = "X('" + BytesUtils.toHexString((byte[]) exampleValue) + "')";
        } else {
          paramValue = exampleValue.toString();
        }
      } else {
        paramValue = getParameterColumnName(signature, i);
      }
      args.add(paramValue);
    }
    return args;
  }

  public static String getParameterColumnName(UdfSignature signature, int argIndex) {
    return getParameterColumnName(signature.getParameters().get(argIndex), argIndex);
  }

  public static String getParameterColumnName(UdfParameter param, int argIndex) {
    return "arg" + argIndex
        + "_" + param.getDataType().name().toLowerCase(Locale.US)
        + (param.isMultivalued() ? "_mv" : "");
  }

  public static String getResultColumnName(UdfSignature signature, NullHandling nullHandling) {
    return getResultColumnName(signature.getReturnType(), nullHandling);
  }

  public static String getResultColumnName(UdfParameter param, NullHandling nullHandling) {
    return "result"
        + "_" + param.getDataType().name().toLowerCase(Locale.US)
        + (param.isMultivalued() ? "_mv" : "")
        + (nullHandling == NullHandling.ENABLED ? "_null" : "");
  }

  public static TableConfig generateTableConfig(List<Udf> udfs) {
    Preconditions.checkArgument(!udfs.isEmpty(), "UDFs list cannot be empty");
    TableConfigBuilder tableConfigBuilder = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName(udfs.get(0)));
    Map<String, IndexConfig> indexConfigs = new HashMap<>();
    indexConfigs.put(StandardIndexes.inverted().getId(), IndexConfig.ENABLED);

    indexConfigs.put(StandardIndexes.dictionary().getId(), IndexConfig.ENABLED);
    JsonNode withDictionary = JsonUtils.objectToJsonNode(indexConfigs);

    tableConfigBuilder.addFieldConfig(
        new FieldConfig.Builder(getTestColumnName())
            .withIndexes(withDictionary)
            .build()
    );
    tableConfigBuilder.addFieldConfig(
        new FieldConfig.Builder(getUdfColumnName())
            .withIndexes(withDictionary)
            .build()
    );
    tableConfigBuilder.addFieldConfig(
        new FieldConfig.Builder(getSignatureColumnName())
            .withIndexes(withDictionary)
            .build()
    );
    Set<String> columnNames = new HashSet<>();

    for (Udf udf : udfs) {
      for (UdfSignature signature : udf.getExamples().keySet()) {
        List<UdfParameter> params = signature.getParameters();
        for (int i = 0; i < params.size(); i++) {
          createColumnInTableConfig(params.get(i), i, tableConfigBuilder, columnNames);
        }
        createResultColsInTableConfig(signature.getReturnType(), tableConfigBuilder, columnNames);
      }
    }

    return tableConfigBuilder.build();
  }

  private static void createColumnInTableConfig(
      UdfParameter parameter,
      int argIndex,
      TableConfigBuilder tableConfigBuilder,
      Set<String> columnNames) {
    String columnName = getParameterColumnName(parameter, argIndex);
    if (!columnNames.contains(columnName)) {
      columnNames.add(columnName);
      updateTableConfig(parameter, tableConfigBuilder, columnName);
    }
  }

  private static void createResultColsInTableConfig(
      UdfParameter parameter,
      TableConfigBuilder tableConfigBuilder,
      Set<String> columnNames) {
    createResultColInTableConfig(parameter, tableConfigBuilder, columnNames, NullHandling.DISABLED);
    createResultColInTableConfig(parameter, tableConfigBuilder, columnNames, NullHandling.ENABLED);
  }

  private static void createResultColInTableConfig(
      UdfParameter parameter,
      TableConfigBuilder tableConfigBuilder,
      Set<String> columnNames,
      NullHandling nullHandling) {
    String columnName = getResultColumnName(parameter, nullHandling);
    if (!columnNames.contains(columnName)) {
      columnNames.add(columnName);
      updateTableConfig(parameter, tableConfigBuilder, columnName);
    }
  }

  public static Schema generateSchema(List<Udf> udfs) {
    Preconditions.checkArgument(!udfs.isEmpty(), "UDFs list cannot be empty");
    Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder();

    schemaBuilder.setSchemaName(getTableName(udfs.get(0)));
    schemaBuilder.setEnableColumnBasedNullHandling(true);
    schemaBuilder.addDimensionField(getTestColumnName(), FieldSpec.DataType.STRING, field -> {
      field.setSingleValueField(true);
      field.setNullable(false);
    });
    schemaBuilder.addDimensionField(getUdfColumnName(), FieldSpec.DataType.STRING, field -> {
      field.setSingleValueField(true);
      field.setNullable(false);
    });
    schemaBuilder.addDimensionField(getSignatureColumnName(), FieldSpec.DataType.STRING, field -> {
      field.setSingleValueField(true);
      field.setNullable(false);
    });

    Set<String> columnNames = new HashSet<>();
    for (Udf udf : udfs) {
      for (UdfSignature signature : udf.getExamples().keySet()) {
        List<UdfParameter> params = signature.getParameters();
        for (int i = 0; i < params.size(); i++) {
          UdfParameter parameter = params.get(i);
          createParamColInSchema(parameter, i, schemaBuilder, columnNames);
        }
        createResultColsInSchema(signature.getReturnType(), schemaBuilder, columnNames);
      }
    }
    return schemaBuilder.build();
  }

  private static void createParamColInSchema(UdfParameter parameter, int argIndex,
      Schema.SchemaBuilder schemaBuilder, Set<String> columnNames) {
    String columnName = getParameterColumnName(parameter, argIndex);
    if (!columnNames.contains(columnName)) {
      columnNames.add(columnName);
      updateSchema(parameter, schemaBuilder, columnName);
    }
  }

  private static void createResultColsInSchema(UdfParameter parameter,
      Schema.SchemaBuilder schemaBuilder, Set<String> columnNames) {
    createResultColInSchema(parameter, schemaBuilder, columnNames, NullHandling.DISABLED);
    createResultColInSchema(parameter, schemaBuilder, columnNames, NullHandling.ENABLED);
  }

  private static void createResultColInSchema(
      UdfParameter parameter,
      Schema.SchemaBuilder schemaBuilder,
      Set<String> columnNames,
      NullHandling nullHandling) {
    String columnName = getResultColumnName(parameter, nullHandling);
    if (!columnNames.contains(columnName)) {
      columnNames.add(columnName);
      updateSchema(parameter, schemaBuilder, columnName);
    }
  }

  public static GenericRow asRow(
      Udf udf,
      UdfSignature signature,
      UdfExample testCase) {
    GenericRow row = new GenericRow();
    row.putValue(getUdfColumnName(), udf.getMainCanonicalName());
    row.putValue(getTestColumnName(), testCase.getId());
    row.putValue(getSignatureColumnName(), signature.toString());
    row.putValue(getResultColumnName(signature, NullHandling.DISABLED), testCase.getResult(NullHandling.DISABLED));
    row.putValue(getResultColumnName(signature, NullHandling.ENABLED), testCase.getResult(NullHandling.ENABLED));
    List<UdfParameter> params = signature.getParameters();
    for (int i = 0; i < params.size(); i++) {
      Object argValue = testCase.getInputValues().get(i);
      String columnName = getParameterColumnName(signature, i);
      if (argValue != null) {
        row.putValue(columnName, argValue);
      } else {
        row.addNullValueField(columnName);
      }
    }

    return row;
  }

  private static void updateSchema(UdfParameter param, Schema.SchemaBuilder schemaBuilder, String columnName) {
    if (param.isMultivalued()) {
      schemaBuilder.addDimensionField(columnName, param.getDataType(),
          fieldSpec -> fieldSpec.setSingleValueField(false));
    } else {
      switch (param.getDataType()) {
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BIG_DECIMAL:
        case BYTES:
          schemaBuilder.addMetricField(columnName, param.getDataType(),
              fieldSpec -> fieldSpec.setSingleValueField(true));
          break;
        case BOOLEAN:
        case TIMESTAMP:
        case STRING:
        case JSON:
          schemaBuilder.addDimensionField(columnName, param.getDataType(),
              fieldSpec -> {
                fieldSpec.setSingleValueField(true);
              });
          break;
        case MAP:
        case LIST:
        case STRUCT:
        case UNKNOWN:
        default:
          throw new IllegalArgumentException("Unsupported data type: " + param.getDataType());
      }
    }
  }

  public static void updateTableConfig(UdfParameter param, TableConfigBuilder tableConfigBuilder, String columnName) {
    List<UdfParameter.Constraint> constraints = param.getConstraints();
    if (!constraints.isEmpty()) {
      for (UdfParameter.Constraint constraint : constraints) {
        constraint.updateTableConfig(tableConfigBuilder, columnName);
      }
      return;
    }
    Map<String, IndexConfig> indexConfigs = new HashMap<>();
    indexConfigs.put(StandardIndexes.inverted().getId(), IndexConfig.ENABLED);
    JsonNode onlyInverted = JsonUtils.objectToJsonNode(indexConfigs);

    tableConfigBuilder.addFieldConfig(
        new FieldConfig.Builder(columnName)
            .withIndexes(onlyInverted)
            .build()
    );
  }
}
