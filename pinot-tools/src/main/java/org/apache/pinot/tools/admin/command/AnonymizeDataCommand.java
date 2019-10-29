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
import org.apache.pinot.tools.PinotDataAndQueryAnonymizer;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({"FieldCanBeLocal", "unused"})
public class AnonymizeDataCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(AnonymizeDataCommand.class);

  @Option(name = "-inputSegmentsDir", metaVar = "<String>", usage = "Absolute path of directory containing Pinot table segments")
  private String _inputSegmentsDir;

  @Option(name = "-outputDir", metaVar = "<String>", usage = "Absolute path of directory where generated Avro files and global dictionaries will be written into")
  private String _outputDir;

  @Option(name = "-avroFileNamePrefix", metaVar = "<String>", usage = "Generated Avro file name prefix")
  private String _avroFileNamePrefix;

  @Option(name = "-generateData", metaVar = "<boolean>", usage = "Should the tool generate data(false by default)")
  private boolean _generateData = false;

  @Option(name = "-generateQueries", metaVar = "<boolean>", usage = "Should the tool generate queries(false by default)")
  private boolean _generateQueries = false;

  @Option(name = "-tableName", metaVar = "<String>", usage = "Table name to use for generating queries")
  private String _tableName;

  @Option(name = "-queryDir", metaVar = "<String>", usage = "Absolute path of directory containing the source query file and where the generated query file will be written into")
  private String _queryDir;

  @Option(name = "-queryFileName", metaVar = "<String>", usage = "Query file name in queryDir")
  private String _queryFileName;

  @Option(name = "-columnsToRetainDataFor", handler = StringArrayOptionHandler.class, usage = "Set of columns to retain data for (empty by default). These should generally be time columns")
  private String[] _columnsToRetainDataFor;

  @Option(name = "-extractFilterColumns", metaVar = "<boolean>", usage = "Should the tool first extract filter columns (false by default)")
  private boolean _extractFilterColumns = false;

  @Option(name = "-filterColumns", handler = StringArrayOptionHandler.class, usage = "Set of filter columns to build global dictionaries for")
  private String[] _columnsParticipatingInFilter;

  @Option(name = "-filterColumnCardinality", handler = StringArrayOptionHandler.class, usage = "filter column cardinalities")
  private String[] _filterColumnCardinalities;

  @Option(name = "-help", help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message")
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
    if (_extractFilterColumns) {
      Set<String> filterColumns = PinotDataAndQueryAnonymizer.FilterColumnExtractor.extractColumnsUsedInFilter(_queryDir, _queryFileName);
      System.out.println("Columns participating in filter");
      for (String column: filterColumns) {
        System.out.println(column);
      }
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
    if (_columnsParticipatingInFilter != null) {
      filterColumns.addAll(Lists.newArrayList(_columnsParticipatingInFilter));
    }

    if (_generateData) {
      // It is fine to not specify any set of filter columns (and cardinalities).
      // In such case, data generation phase will skip building global dictionaries
      // and simply generate random data. Later during query generation, we will assert
      // if we encounter a WHERE clause. So user should choose to not specify the set of
      // filter columns only if they are interested in generating any arbitrary data
      // without taking care of input Pinot segment distribution/cardinality and they
      // won't be generating queries later.
      //
      // However, we check for the condition that if user has specified a set of
      // filter columns, the corresponding cardinalities are also there in equal number
      if ((_columnsParticipatingInFilter != null && _filterColumnCardinalities == null) ||
          (_filterColumnCardinalities != null && _columnsParticipatingInFilter == null) ||
          (_columnsParticipatingInFilter != null && _columnsParticipatingInFilter.length != _filterColumnCardinalities.length)) {
        throw new RuntimeException("Please correctly specify the set of filter columns and their corresponding cardinality values");
      }

      Map<String, Integer> filterColumnCardinalityMap = new HashMap<>();

      if (_columnsParticipatingInFilter != null) {
        for (int i = 0; i < _columnsParticipatingInFilter.length; i++) {
          String filterColumn = _columnsParticipatingInFilter[i];
          int filterColumnCardinality = Integer.valueOf(_filterColumnCardinalities[i]);
          filterColumnCardinalityMap.put(filterColumn, filterColumnCardinality);
        }
      }

      // generate data
      PinotDataAndQueryAnonymizer pinotDataGenerator = new PinotDataAndQueryAnonymizer(
          _inputSegmentsDir,
          _outputDir,
          _avroFileNamePrefix,
          filterColumnCardinalityMap,
          columnsToRetainDataFor);
      // first build global dictionaries
      pinotDataGenerator.buildGlobalDictionaries();
      // use global dictionaries to generate Avro files
      pinotDataGenerator.generateAvroFiles();

      return true;
    }

    if (_generateQueries) {
      // generate queries from prebuilt global dictionaries
      PinotDataAndQueryAnonymizer.QueryGenerator queryGenerator = new PinotDataAndQueryAnonymizer.QueryGenerator(
          _outputDir, _queryDir, _queryFileName, _tableName, filterColumns, columnsToRetainDataFor);
      queryGenerator.generateQueries();

      return true;
    }

    throw new RuntimeException(
        "One of the options (-extractFilterColumns, -generateDataa, -generateQueries should be true. Please use the -help option to see usage examples");
  }

  @Override
  public String description() {
    return "Data anonymizer";
  }

  @Override
  public void printExamples() {
    StringBuilder usage = new StringBuilder();

    usage.append("\n*******Usage Notes******")
        .append("\n\nData anonymizer can be used as a sequence of multiple steps to anonymize and generate random data.")
        .append("\nSTEP 1 - user should first extract the set of filter columns. The tool will parse the query file")
        .append("\nin queryDir and output the names of columns that participate in WHERE clause")
        .append("\n\nsh pinot-admin.sh -queryDir /home/user/queryDir -queryFileName queries.raw -extractFilterColumns")
        .append("\n\nSTEP 2 - Provide the set of filter columns (extracted in STEP 1) as input here and anonymize input data.")
        .append("\nInput data is set of Pinot segments. The tool will build global dictionaries for the set of filter")
        .append("\ncolumns to keep the distribution of values same. For other columns, random arbitrary data will")
        .append("\nbe generated. The generated data will be written in Avro files (1 per input segment) in the outputDir.")
        .append("\nThe global dictionaries and anonymous column name mapping will also be written in the same directory.")
        .append("\n\nsh pinot-admin.sh -inputSegmentsDir /home/user/pinotTable/segmentDir -outputDir /home/user/outputDir -avroFileNamePrefix Foo -filterColumns col1 col2 -filterColumnCardinality 100000 50000 -generateData")
        .append("\n\nSTEP 3 - Generate queries. The global dictionaries built in STEP 2 will now be used to generate PQL/SQL queries")
        .append("\non anonymized data. The use of global dictionaries helps to preserve the query patterns. The tool will parse")
        .append("\nthe query file in queryDir and write a file with name queries.generated in the same directory.")
        .append("\n\nsh pinot-admin.sh -outputDir /home/user/outputDir -queryDir /home/user/queryDir -queryFileName queries.raw -tableName MyTable -filterColumns col1 col2 -generateQueries")
        .append("\n\nAs part of STEP 2 and STEP 3, user can also specify a set of columns for which the data needs to be retained.")
        .append("\nThus no global dictionaries will be built for such columns, column value will be copied as is from input")
        .append("\npinot segment into the Avro file. The query generation phase will also copy the value as is into")
        .append("\nwhen generating queries if such column happens to be part of WHERE clause. Usually such columns")
        .append("\nare time (or time related) columns where anonymizing data is not important as the time column")
        .append("\nby itself does not reveal anything. Specify the set of columns as -columnsToRetainDataFor col7 col8")
        .append("\n\nUser can choose to skip STEP 1 and then not provide the names of filter columns in STEP 2.")
        .append("\nIn such case, the tool will operate in pure random data generation mode without taking care")
        .append("\nof data distribution and cardinalities as global dictionaries can't be built. Similarly")
        .append("\nlater during query generation, the tool will assert if it encounters a WHERE clause. It is recommended")
        .append("\nto do this only when the user is interested in pure arbitrary data generation and will not be generating queries");

    System.out.println(usage.toString());
  }
}
