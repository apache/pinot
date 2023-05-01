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
package org.apache.pinot.tools.admin.command;

import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.anonymizer.PinotDataAndQueryAnonymizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@SuppressWarnings({"FieldCanBeLocal", "unused"})
@CommandLine.Command(name = "AnonymizeData", description = "Tool to anonymize a given Pinot table data while "
                                                           + "preserving data characteristics and query patterns",
    mixinStandardHelpOptions = true)
public class AnonymizeDataCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(AnonymizeDataCommand.class);

  @CommandLine.Option(names = {"-action"}, required = true,
      description = "Can take one of the 3 values: (extractFilterColumns, generateData, generateQueries) depending on "
          + "whether the tool should extract the filter columns, generate data or generate queries")
  private String _action;

  @CommandLine.Option(names = {"-inputSegmentsDir"},
      description = "Absolute path of directory containing Pinot table segments")
  private String _inputSegmentsDir;

  @CommandLine.Option(names = {"-outputDir"}, description = "Absolute path of directory where generated Avro files"
      + " and global dictionaries will be written into")
  private String _outputDir;

  @CommandLine.Option(names = {"-avroFileNamePrefix"}, description = "Generated Avro file name prefix")
  private String _avroFileNamePrefix;

  @CommandLine.Option(names = {"-tableName"}, description = "Table name to use for generating queries")
  private String _tableName;

  @CommandLine.Option(names = {"-queryDir"},
      description = "Absolute path of directory containing the original query file and where the generated query file"
          + " will be written into")
  private String _queryDir;

  @CommandLine.Option(names = {"-originalQueryFile"}, description = "Original query file name in queryDir")
  private String _originalQueryFile;

  @CommandLine.Option(names = {"-columnsToRetainDataFor"}, arity = "1..*",
      description = "Set of columns to retain data for (empty by default). Values of these columns will not be "
          + "anonymised. These should be columns containing values of time(e.g daysSinceEpoch, hoursSinceEpoch etc)")
  private String[] _columnsToRetainDataFor;

  @CommandLine.Option(names = {"-filterColumns"}, arity = "1..*",
      description = "Set of filter columns and their cardinalities. Global dictionaries will be built for these "
          + "columns. Use -help option to see usage example")
  private String[] _columnsParticipatingInFilter;

  @CommandLine.Option(names = {"-mapBasedGlobalDictionaries"},
      description = "Whether to use map based global dictionary for improved performance of building global dictionary"
          + " but with additional heap overhead. True by default")
  private boolean _mapBasedGlobalDictionaries = true;

  @Override
  public String getName() {
    return "AnonymizeData";
  }

  @Override
  public boolean execute()
      throws Exception {
    if (_action.equalsIgnoreCase("extractFilterColumns")) {
      Set<String> filterColumns =
          PinotDataAndQueryAnonymizer.FilterColumnExtractor.extractColumnsUsedInFilter(_queryDir, _originalQueryFile);
      StringBuilder sb = new StringBuilder();
      sb.append("Columns participating in filter: ");
      for (String column : filterColumns) {
        sb.append(column).append(" ");
      }
      System.out.println(sb.toString());
      // if the user has asked for extracting filter columns from a query file, then
      // we should simply return after doing that since the tool will be run subsequently
      // based on the set of filter columns it returns to the user
      return true;
    }

    Set<String> columnsToRetainDataFor = new HashSet<>();
    if (_columnsToRetainDataFor != null) {
      columnsToRetainDataFor.addAll(Lists.newArrayList(_columnsToRetainDataFor));
    }

    Set<String> filterColumns = new HashSet<>();
    Map<String, Integer> filterColumnCardinalityMap = new HashMap<>();
    if (_columnsParticipatingInFilter != null) {
      for (int i = 0; i < _columnsParticipatingInFilter.length; i++) {
        String columnInfo = _columnsParticipatingInFilter[i];
        String[] columnAndCardinality = columnInfo.split(":");
        int cardinality = Integer.valueOf(columnAndCardinality[1]);
        filterColumnCardinalityMap.put(columnAndCardinality[0], cardinality);
        filterColumns.add(columnAndCardinality[0]);
      }
    }

    if (_action.equalsIgnoreCase("generateData")) {
      // generate data
      PinotDataAndQueryAnonymizer pinotDataGenerator =
          new PinotDataAndQueryAnonymizer(_inputSegmentsDir, _outputDir, _avroFileNamePrefix,
              filterColumnCardinalityMap, columnsToRetainDataFor, _mapBasedGlobalDictionaries);
      // first build global dictionaries
      pinotDataGenerator.buildGlobalDictionaries();
      // use global dictionaries to generate Avro files
      pinotDataGenerator.generateAvroFiles();

      return true;
    }

    if (_action.equalsIgnoreCase("generateQueries")) {
      throw new UnsupportedOperationException("generateQueries action not supported yet");

      // TODO: Uncomment after re-implementing the SQL query generator
//      // generate queries from prebuilt global dictionaries
//      PinotDataAndQueryAnonymizer.QueryGenerator queryGenerator =
//          new PinotDataAndQueryAnonymizer.QueryGenerator(_outputDir, _queryDir, _originalQueryFile, _tableName,
//              filterColumns, columnsToRetainDataFor);
//      queryGenerator.generateQueries();
//
//      return true;
    }

    throw new RuntimeException(
        "-action should be either extractFilterColumns or generateData or generateQueries. Please use the -help "
            + "option to see usage examples");
  }
}
