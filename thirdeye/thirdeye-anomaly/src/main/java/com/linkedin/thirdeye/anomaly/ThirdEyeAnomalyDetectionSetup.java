package com.linkedin.thirdeye.anomaly;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyDetectionConfiguration.Mode;
import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionDriverConfig;
import com.linkedin.thirdeye.anomaly.database.AnomalyTable;
import com.linkedin.thirdeye.anomaly.util.DimensionSpecUtils;
import com.linkedin.thirdeye.anomaly.util.MetricSpecUtils;
import com.linkedin.thirdeye.anomaly.util.ResourceUtils;
import com.linkedin.thirdeye.anomaly.util.ServerUtils;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;

/**
 * This class does everything required to get started with thirdeye-anomaly, creating config files and populating the
 * database.
 */
public class ThirdEyeAnomalyDetectionSetup {

  /**
   * Setup config files and database
   */
  public void setup() throws Exception {
    Scanner userInput = new Scanner(System.in);
    System.out.println(
        "Welcome to thirdeye-anomaly setup. This process will guide "
            + "you through the process of generating base configuration "
            + "templates and database tables.");

    ThirdEyeAnomalyDetectionConfiguration topLevelConfig = getTopLevelConfig(userInput);

    // get the star tree config (for sanity checking + automated config)
    StarTreeConfig starTreeConfig = ServerUtils.getStarTreeConfig(topLevelConfig.getThirdEyeServerHost(),
        topLevelConfig.getThirdEyeServerPort(), topLevelConfig.getCollectionName());

    topLevelConfig.setAnomalyDatabaseConfig(getDatabaseConfig(userInput));
    topLevelConfig.setDriverConfig(getDriverConfig(userInput, starTreeConfig));

    // preview the config file
    YAMLFactory yamlFactory = new YAMLFactory();
    ObjectMapper mapper = new ObjectMapper(yamlFactory);
    mapper.setSerializationInclusion(Include.NON_NULL);
    System.out.println(mapper.writeValueAsString(topLevelConfig));
    if (!promptForYN("Accept this configuration?", userInput)) {
      System.err.println("Abort...");
      System.exit(0);
    }

    // write the config to file
    String pathName = promptForInput("Enter a filename (config.yml)", userInput);
    File configFile = new File(pathName);
    File parent = configFile.getParentFile();
    if(parent != null && !parent.exists() && !parent.mkdirs()){
      throw new IllegalStateException("Couldn't create dir: " + parent);
    }
    yamlFactory.createGenerator(new FileOutputStream(configFile)).writeObject(topLevelConfig);

    // create tables and insert default function configuration into the database
    switch (topLevelConfig.getMode()) {
      case GENERIC:
      {
        initializeGenericAnomalyDatabase(topLevelConfig, userInput, starTreeConfig);
        break;
      }
      case RULEBASED:
      {
        initializeRuleBasedAnomalyDatabase(topLevelConfig, userInput, starTreeConfig);
        break;
      }
    }

    printSectionTitle("Congratulations, you are done!");
    System.out.println("Config file has been saved to: " + configFile.getAbsolutePath());
    System.out.println("usage: java -jar <this_jar> <your_config.yml>");
    // print message about dashboard setup
    printDashboardMessage(topLevelConfig.getAnomalyDatabaseConfig(), mapper);
  }

  /**
   * Print instructions for getting anomaly results to show in the thirdeye-dashboard
   */
  private void printDashboardMessage(AnomalyDatabaseConfig anomalyDatabaseConfig, ObjectMapper mapper)
      throws JsonProcessingException {
    System.out.println();
    System.out.println("If you would like anomalies to be shown in the thirdeye-dashboard,\n"
        + "you will need to add the following to your dashboard config.yml");
    System.out.println();

    String[] lines = mapper.writeValueAsString(
        new AnomalyDatabaseConfig(anomalyDatabaseConfig.getUrl(),
            null,
            anomalyDatabaseConfig.getAnomalyTableName(),
            anomalyDatabaseConfig.getUser(),
            anomalyDatabaseConfig.getPassword(),
            true)).split("\n");

    System.out.println("anomalyDatabaseConfig:");
    // 0-th line is '---'
    for (int i = 1; i < lines.length; i++) {
      System.out.println("  " + lines[i]);
    }
  }

  /**
   * Setup database tables for rule based detection
   */
  private void initializeRuleBasedAnomalyDatabase(ThirdEyeAnomalyDetectionConfiguration config,
      Scanner userInput, StarTreeConfig starTreeConfig) throws IOException {
    printSectionTitle("basic mysql setup");

    if (promptForYN("Create the function table?", userInput)) {
      String sql = String.format(ResourceUtils.getResourceAsString("database/rulebased/create-rule-table.sql"),
          config.getAnomalyDatabaseConfig().getFunctionTableName());
      if (!config.getAnomalyDatabaseConfig().runSQL(sql)) {
        if (!promptForYN("An error occured, do you wish to continue?", userInput)) {
          System.err.println("Abort...");
          System.exit(1);
        } else {
          printSectionTitle("Here is the sql for your reference");
          System.out.println(sql);
        }
      }
    }

    if (promptForYN("Create the anomaly table?", userInput)) {
      AnomalyTable.createTable(config.getAnomalyDatabaseConfig());
    }

    // TODO : add default templates for rule based detection
  }

  /**
   * Set up generic anomaly detection tables along with default kalman algorithm.
   */
  private void initializeGenericAnomalyDatabase(ThirdEyeAnomalyDetectionConfiguration config, Scanner userInput,
      StarTreeConfig starTreeConfig) throws IOException {
    printSectionTitle("basic mysql setup");

    if (promptForYN("Create the function table?", userInput)) {
      String sql = String.format(ResourceUtils.getResourceAsString("database/generic/create-function-table.sql"),
          config.getAnomalyDatabaseConfig().getFunctionTableName());
      if (!config.getAnomalyDatabaseConfig().runSQL(sql)) {
        if (!promptForYN("An error occured, do you wish to continue?", userInput)) {
          System.err.println("Abort...");
          System.exit(1);
        } else {
          printSectionTitle("Here is the sql for your reference");
          System.out.println(sql);
        }
      }
    }

    if (promptForYN("Create the anomaly table?", userInput)) {
      AnomalyTable.createTable(config.getAnomalyDatabaseConfig());
    }

    if (promptForYN("Do you want to load the default (Kalman filter) algorithm?", userInput)) {
      String metricsString = promptForInput(
          "Metrics you want to analyze (delimited by ',', e.g., \"m1,m2,m3\").\n" +
          "If empty, then all metrics will be monitored.", userInput);

      List<String> validMetrics = MetricSpecUtils.getMetricNames(starTreeConfig.getMetrics());

      List<String> metrics;
      if (metricsString.equals("")) {
        // add all metrics
        metrics = validMetrics;
      } else {
        // add only selected metrics
        metrics = Arrays.asList(metricsString.split(","));
        for (String metric : metrics) {
          if (!validMetrics.contains(metric)) {
            System.err.println("'" + metric + "' is not a valid metric in " + starTreeConfig.getCollection());
            System.exit(1);
          }
        }
      }

      printSectionTitle("inserting functions");
      for (String metric : metrics) {
        String sql = String.format(ResourceUtils.getResourceAsString(
            "database/generic/insert-kalman-filter-default.sql"),
            config.getAnomalyDatabaseConfig().getFunctionTableName(),
            config.getCollectionName(),
            metric.trim());
        System.out.println(sql);
        config.getAnomalyDatabaseConfig().runSQL(sql);
      }
    }
  }

  /**
   * @param userInput
   * @return
   *  Top level thirdeye-anomaly settings
   */
  private ThirdEyeAnomalyDetectionConfiguration getTopLevelConfig(Scanner userInput) {
    printSectionTitle("thirdeye-anomaly");
    Mode mode = Mode.valueOf(promptForInput("Mode of anomaly detection? (GENERIC or RULEBASED)",userInput).toUpperCase());
    String collection = promptForInput("What is the collection name?", userInput);
    String host = promptForInput("What is the thirdeye-server's hostname?", userInput);
    Short port = Short.valueOf(promptForInput("What is the thirdeye-server's applicationPort?", userInput));
    ThirdEyeAnomalyDetectionConfiguration baseConfig = new ThirdEyeAnomalyDetectionConfiguration();
    baseConfig.setMode(mode);
    baseConfig.setCollectionName(collection);
    baseConfig.setThirdEyeServerHost(host);
    baseConfig.setThirdEyeServerPort(port);
    baseConfig.setDetectionInterval(new TimeGranularity(1, TimeUnit.HOURS));
    return baseConfig;
  }

  /**
   * @param userInput
   * @return
   *  Driver settings
   */
  private AnomalyDetectionDriverConfig getDriverConfig(Scanner userInput, StarTreeConfig starTreeConfig) {
    printSectionTitle("driver configuration");
    System.out.println("These settings determine how thirdeye-anomaly will explore various dimension combinations.");

    String contributionEstimateMetric = promptForInput("What metric to use to estimate the importance of "
        + "a dimension combination?", userInput);
    int maxExplorationDepth = Integer.valueOf(promptForInput("How many dimensions should the driver attempt to "
        + "fix/group by? (recommend 1)", userInput));
    String[] dimensionPrecedence = promptForInput(
        "Enter the precedence of dimensions (delimited by ',') that the driver should explore (not aliases!).\n"
        + "For efficiency, we recommend listing dimensions with high cardinality first.(e.g., \"d1,d2,d3\")\n",
        userInput).split(",");

    List<String> validDimensions = DimensionSpecUtils.getDimensionNames(starTreeConfig.getDimensions());
    for (int i = 0; i < dimensionPrecedence.length; i++) {
      dimensionPrecedence[i] = dimensionPrecedence[i].trim();
      if (!validDimensions.contains(dimensionPrecedence[i])) {
        System.err.println("'" + dimensionPrecedence[i] + "' is not a valid dimension in "
            + starTreeConfig.getCollection());
        System.exit(1);
      }
    }

    AnomalyDetectionDriverConfig driverConfig = new AnomalyDetectionDriverConfig();
    driverConfig.setDimensionPrecedence(Arrays.asList(dimensionPrecedence));
    driverConfig.setContributionEstimateMetric(contributionEstimateMetric);
    driverConfig.setMaxExplorationDepth(maxExplorationDepth);
    return driverConfig;
  }

  /**
   * @param userInput
   * @return
   *  Anomaly database settings
   */
  private AnomalyDatabaseConfig getDatabaseConfig(Scanner userInput) {
    printSectionTitle("anomaly database");
    System.out.println("thirdeye-anomaly uses a mysql database to store function definitions and publish"
        + " anomaly output.");

    String url = promptForInput("What is your database? (e.g., localhost/thirdeye)", userInput);
    String user = promptForInput("  username", userInput);
    String password = promptForInput("  password", userInput);
    String functionTableName = promptForInput("Specify a table to store function definitions (e.g., ads_functions)",
        userInput);
    String anomalyTableName = promptForInput("Specify a table to publish anomalies to (e.g., anomaly)",
        userInput);

    AnomalyDatabaseConfig dbConfig = new AnomalyDatabaseConfig(url, functionTableName, anomalyTableName, user,
        password, true);
    return dbConfig;
  }

  /*
   * Helper methods for user io
   */

  private static boolean promptForYN(String text, Scanner userInput) {
    return promptForInput(text + " (y/n)", userInput).toLowerCase().charAt(0) == 'y';
  }

  private static String promptForInput(String text, Scanner userInput) {
    System.out.print(text + ": ");
    return userInput.nextLine();
  }

  private static void printSectionTitle(String title) {
    for (int i = 0; i < title.length(); i++) {
      System.out.print('=');
    }
    System.out.print('\n');
    System.out.println(title);
    for (int i = 0; i < title.length(); i++) {
      System.out.print('=');
    }
    System.out.print('\n');
  }

}
