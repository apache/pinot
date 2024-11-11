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
@CommandLine.Command(name = "AnonymizeData", mixinStandardHelpOptions = true)
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

  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, help = true, description = "Print this message")
  private boolean _help = false;

  public boolean getHelp() {
    return _help;
  }

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

  @Override
  public String description() {
    return "Tool to anonymize a given Pinot table data while preserving data characteristics and query patterns";
  }

  @Override
  public void printExamples() {
    StringBuilder usage = new StringBuilder();

    usage.append("\n*******Usage Notes******").append(
        "\n\nData and Query Anonymizer can be used as a sequence of multiple steps to anonymize input Pinot table "
            + "data and generate queries for anonymized data").append(
        "\nSTEP 1 - user should first extract the set of filter columns. The tool will parse the original query "
            + "file").append("\nin queryDir and output the names of columns that participate in WHERE clause").append(
        "\n\nsh pinot-admin.sh AnonymizeData -queryDir /home/user/queryDir -originalQueryFile queries.raw -action "
            + "extractFilterColumns").append(
        "\n\nSTEP 2 - Provide the set of filter columns (extracted in STEP 1) along with their cardinalities as "
            + "input here and anonymize input data.").append(
        "\nInput data is a set of Pinot segments. The tool will build global dictionaries for the set of filter")
        .append("\ncolumns to keep the distribution of values same. Global dictionary for each filter column will")
        .append(
            "\ncontain a 1-1 mapping between original column value and corresponding random value. For other columns,"
                + " random arbitrary data will").append(
        "\nbe generated. The generated data will be written in Avro files (1 per input segment) in the outputDir.")
        .append(
            "\nThe global dictionaries and anonymous column name mapping will also be written in the same directory.")
        .append("\n\nsh pinot-admin.sh AnonymizeData -inputSegmentsDir /home/user/pinotTable/segmentDir -outputDir "
            + "/home/user/outputDir -avroFileNamePrefix Foo -filterColumns col1:100000 col2:50000 -action "
            + "generateData").append(
        "\n\nSTEP 3 - Generate queries. The global dictionaries built in STEP 2 will now be used to generate queries")
        .append("\non anonymized data. The use of global dictionaries helps to preserve the query patterns. The tool "
            + "will parse").append(
        "\nthe original query file in queryDir and write a file with name queries.generated in the same directory.")
        .append(
            "\nThe global dictionaries will help in rewriting the predicates by substituting the original value with "
                + "the corresponding anonymous value").append(
        "\n\nsh pinot-admin.sh AnonymizeData -outputDir /home/user/outputDir -queryDir /home/user/queryDir "
            + "-queryFileName queries.raw -tableName MyTable -filterColumns col1:100000 col2:50000 -action "
            + "generateQueries").append(
        "\n\nAs part of STEP 2 and STEP 3, user can also specify a set of columns for which the data needs to be "
            + "retained.").append(
        "\nThus no global dictionaries will be built for such columns, column value will be copied as is from "
            + "input")
        .append("\npinot segment into the Avro file. The query generation phase will also copy the value as is")
        .append("\nwhen generating queries if such column happens to be part of WHERE clause. Usually such columns")
        .append("\nare time (or time related) columns where anonymizing data is not important as the time column")
        .append("\nby itself does not reveal anything. Specify the set of columns as -columnsToRetainDataFor col7 col8")
        .append("\n\nUser can choose to skip STEP 1 and then not provide the names of filter columns in STEP 2.")
        .append("\nIn such case, the tool will operate in pure random data generation mode without taking care")
        .append("\nof data distribution and cardinalities as global dictionaries can't be built. Similarly").append(
        "\nlater during query generation, the tool will assert if it encounters a WHERE clause. It is recommended")
        .append(
            "\nto do this only when the user is interested in pure arbitrary data generation and will not be "
                + "generating queries");

    System.out.println(usage.toString());
  }
}
