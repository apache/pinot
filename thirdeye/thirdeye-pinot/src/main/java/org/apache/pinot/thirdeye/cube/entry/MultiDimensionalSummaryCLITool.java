/*
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

package org.apache.pinot.thirdeye.cube.entry;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.cube.additive.AdditiveDBClient;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.apache.pinot.thirdeye.cube.cost.BalancedCostFunction;
import org.apache.pinot.thirdeye.cube.cost.CostFunction;
import org.apache.pinot.thirdeye.dashboard.Utils;
import org.apache.pinot.thirdeye.cube.summary.SummaryResponse;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.common.constants.rca.MultiDimensionalSummaryConstants.*;
import static org.apache.pinot.thirdeye.common.constants.rca.RootCauseResourceConstants.*;


public class MultiDimensionalSummaryCLITool {
  private static final Logger LOG = LoggerFactory.getLogger(MultiDimensionalSummaryCLITool.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();

  public static final String TOP_K_POSTFIX = "_topk";

  private static Options buildOptions() {
    Options options = new Options();

    Option dataset = Option.builder("dataset").desc("dataset name").hasArg().argName("NAME").required().build();
    options.addOption(dataset);

    Option metricName = Option.builder("metric").desc("metric name").hasArg().argName("NAME").required().build();
    options.addOption(metricName);

    Option dimensions =
        Option.builder("dim").longOpt("dimensions").desc("dimension names that are separated by comma").hasArg()
            .argName("LIST").build();
    options.addOption(dimensions);

    Option excludedDimensions =
        Option.builder("notDim").longOpt(CUBE_EXCLUDED_DIMENSIONS).desc("dimension names to be excluded").hasArg()
            .argName("LIST").build();
    options.addOption(excludedDimensions);

    Option filters =
        Option.builder("filters").desc("filter to apply on the data cube (a map of list in Json format)").hasArg().argName("JSON").build();
    options.addOption(filters);

    Option currentStart =
        Option.builder("cstart").longOpt(CURRENT_START).desc("current start time inclusive").hasArg().argName("MILLIS")
            .required().build();
    options.addOption(currentStart);

    Option currentEnd =
        Option.builder("cend").longOpt(CURRENT_END).desc("current end time exclusive").hasArg().argName("MILLIS")
            .required().build();
    options.addOption(currentEnd);

    Option baselineStart =
        Option.builder("bstart").longOpt(BASELINE_START).desc("baseline start time inclusive").hasArg()
            .argName("MILLIS").required().build();
    options.addOption(baselineStart);

    Option baselineEnd =
        Option.builder("bend").longOpt(BASELINE_END).desc("baseline end time exclusive").hasArg().argName("MILLIS")
            .required().build();
    options.addOption(baselineEnd);

    Option size =
        Option.builder("size").longOpt(CUBE_SUMMARY_SIZE).desc("size of summary").hasArg().argName("NUMBER").build();
    options.addOption(size);

    Option depth = Option.builder(CUBE_DEPTH).desc("number of top dimensions").hasArg().argName("NUMBER").build();
    options.addOption(depth);

    Option hierarchies = Option.builder("h").longOpt(CUBE_DIM_HIERARCHIES)
        .desc("dimension hierarchies (a list of lists in Json format)").hasArg().argName("JSON")
        .build();
    options.addOption(hierarchies);

    Option oneSideError = Option.builder(CUBE_ONE_SIDE_ERROR).desc("enable one side error summary").build();
    options.addOption(oneSideError);

    Option manualOrder = Option.builder(CUBE_MANUAL_ORDER).desc("use manual dimension order").build();
    options.addOption(manualOrder);

    Option dateTimeZone =
        Option.builder(TIME_ZONE).desc("time zone id in Joda library").hasArg().argName("ID").build();
    options.addOption(dateTimeZone);

    Option costFunctionClass = Option.builder("cost").longOpt("costFunction").desc(
        "the parameters of the cost function (a map in json format) "
            + "Essential field in the map: 'className'").hasArg().argName("JSON").build();
    options.addOption(costFunctionClass);

    return options;
  }

  /**
   * Removes noisy dimensions.
   *
   * @param dimensions the original dimensions.
   *
   * @return the original dimensions minus noisy dimensions, which are predefined.
   *
   * TODO: Replace with an user configurable method
   */
  public static Dimensions sanitizeDimensions(Dimensions dimensions) {
    List<String> allDimensionNames = dimensions.names();
    Set<String> dimensionsToRemove = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    dimensionsToRemove.add("environment");
    dimensionsToRemove.add("colo");
    dimensionsToRemove.add("fabric");
    for (String dimensionName : allDimensionNames) {
      if(dimensionName.contains(TOP_K_POSTFIX)) {
        String rawDimensionName = dimensionName.replaceAll(TOP_K_POSTFIX, "");
        dimensionsToRemove.add(rawDimensionName.toLowerCase());
      }
    }
    return removeDimensions(dimensions, dimensionsToRemove);
  }

  public static Dimensions removeDimensions(Dimensions dimensions, Collection<String> dimensionsToRemove) {
    List<String> dimensionsToRetain = new ArrayList<>();
    for (String dimensionName : dimensions.names()) {
      if(!dimensionsToRemove.contains(dimensionName.trim())){
        dimensionsToRetain.add(dimensionName);
      }
    }
    return new Dimensions(dimensionsToRetain);
  }

  public static CostFunction initiateCostFunction(String paramString)
      throws IOException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
             InvocationTargetException, InstantiationException {
    HashMap<String, String> params = objectMapper.readValue(paramString, HashMap.class);

    String className = params.get("className");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(className), "Class name of cost function cannot be empty.");

    Class<CostFunction> clazz = (Class<CostFunction>) Class.forName(className);
    Constructor<CostFunction> constructor = clazz.getConstructor(Map.class);
    return constructor.newInstance(new Object[] { params });
  }

  public static void main(String[] args) throws Exception {
    Options options = buildOptions();
    if (args.length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(MultiDimensionalSummary.class.getSimpleName() + " thirdeye-configs-path [options]", options);
    } else {
      CommandLineParser parser = new DefaultParser();
      CommandLine commandLine = parser.parse(options, args);
      List<String> argList = commandLine.getArgList();
      Preconditions.checkArgument(argList.size() > 0, "Please provide config directory as parameter");

      // Get parameters from command line arguments
      String dataset = commandLine.getOptionValue("dataset");
      String metricName = commandLine.getOptionValue("metric");
      String dimensionString = commandLine.getOptionValue("dimensionString", "");
      String excludedDimensionString = commandLine.getOptionValue(CUBE_EXCLUDED_DIMENSIONS, "");
      String filterJson = commandLine.getOptionValue("filters", "{}");
      long currentStart = Long.parseLong(commandLine.getOptionValue(CURRENT_START));
      long currentEnd = Long.parseLong(commandLine.getOptionValue(CURRENT_END));
      long baselineStart = Long.parseLong(commandLine.getOptionValue(BASELINE_START));
      long baselineEnd = Long.parseLong(commandLine.getOptionValue(BASELINE_END));

      int summarySize = Integer.parseInt(commandLine.getOptionValue("size", "10"));
      int depth = Integer.parseInt(commandLine.getOptionValue(CUBE_DEPTH, "3"));
      String hierarchiesJson = commandLine.getOptionValue(CUBE_DIM_HIERARCHIES, "[]");
      if (commandLine.hasOption("manual")) {
        depth = 0;
      }
      boolean oneSideError = commandLine.hasOption(CUBE_ONE_SIDE_ERROR);
      String dateTimeZoneId = commandLine.getOptionValue(TIME_ZONE, DateTimeZone.UTC.getID());
      DateTimeZone timeZone = DateTimeZone.forID(dateTimeZoneId);

      // Create cost function
      CostFunction costFunction = null;
      if (commandLine.hasOption("costFunction")) {
        costFunction = initiateCostFunction(commandLine.getOptionValue("costFunction"));
      } else {
        costFunction = new BalancedCostFunction();
      }
      LOG.info("Using cost function '{}' for summary algorithm.", costFunction.getClass().getSimpleName());
      Preconditions.checkNotNull(costFunction);

      // Initialize ThirdEye's environment
      ThirdEyeUtils.initLightWeightThirdEyeEnvironment(argList.get(0));
      AdditiveDBClient cubeDbClient = new AdditiveDBClient(CACHE_REGISTRY_INSTANCE.getQueryCache());

      // Convert JSON string to Objects
      Dimensions dimensions;
      if (Strings.isNullOrEmpty(dimensionString)) {
        dimensions = sanitizeDimensions(new Dimensions(Utils.getSchemaDimensionNames(dataset)));
      } else {
        dimensions = new Dimensions(Arrays.asList(dimensionString.trim().split(",")));
      }
      if (!Strings.isNullOrEmpty(excludedDimensionString)) {
        List<String> dimensionsToBeRemoved = Arrays.asList(excludedDimensionString.trim().split(","));
        dimensions = removeDimensions(dimensions, dimensionsToBeRemoved);
      }

      Multimap<String, String> dataFilter = ThirdEyeUtils.convertToMultiMap(filterJson);
      List<List<String>> hierarchies = objectMapper.readValue(hierarchiesJson, new TypeReference<List<List<String>>>() {
      });

      // Trigger summary algorithm
      MultiDimensionalSummary mdSummary = new MultiDimensionalSummary(cubeDbClient, costFunction, timeZone);
      SummaryResponse summaryResponse = mdSummary
          .buildSummary(dataset, metricName, currentStart, currentEnd, baselineStart, baselineEnd, dimensions,
              dataFilter, summarySize, depth, hierarchies, oneSideError);

      // Log summary result
      LOG.info(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(summaryResponse));
    }

    // Force closing the connections to data sources.
    System.exit(0);
  }
}
