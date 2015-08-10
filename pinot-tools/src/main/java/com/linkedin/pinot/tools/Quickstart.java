/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools;

import java.io.File;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.tools.admin.command.QuickstartRunner;


public class Quickstart {
  private File _quickStartDataDir;

  public Quickstart() {

  }

  public enum color {
    GREEN("\u001B[32m"),
    YELLOW("\u001B[33m"),
    CYAN("\u001B[36m");
    private String code;

    color(String code) {
      this.code = code;
    }
  }

  public static void printStatus(color code, String message) {
    System.out.println(code.code + message + code.code);
  }

  public static String prettyprintResponse(JSONObject response) throws JSONException {
    final String TAB = "\t\t";
    final String NEW_LINE = "\n";
    StringBuilder bld = new StringBuilder();

    if (response.has("selectionResults") && response.getJSONObject("selectionResults") != null) {
      JSONArray columns = response.getJSONObject("selectionResults").getJSONArray("columns");
      String[] columnsArr = new String[columns.length()];
      for (int i = 0; i < columns.length(); i++) {
        columnsArr[i] = columns.getString(i);
      }
      bld.append(StringUtils.join(columnsArr, TAB) + NEW_LINE);
      JSONArray rows = response.getJSONObject("selectionResults").getJSONArray("results");
      for (int i = 0; i < rows.length(); i++) {
        JSONArray a = rows.getJSONArray(i);
        String[] rowsA = new String[a.length()];
        for (int j = 0; j < a.length(); j++) {
          rowsA[j] = a.getString(j);
        }
        bld.append(StringUtils.join(rowsA, TAB) + NEW_LINE);
      }
      return bld.toString();
    } else if (response.has("aggregationResults") && response.getJSONArray("aggregationResults").length() > 0
        && !response.getJSONArray("aggregationResults").getJSONObject(0).has("groupByResult")) {
      JSONArray aggs = response.getJSONArray("aggregationResults");
      for (int i = 0; i < aggs.length(); i++) {
        bld.append(aggs.getJSONObject(i).getString("function") + TAB);
      }
      bld.append(NEW_LINE);
      for (int i = 0; i < aggs.length(); i++) {
        bld.append(aggs.getJSONObject(i).getString("value") + TAB);
      }
      return bld.toString();
    } else {

      JSONObject groupByResult = response.getJSONArray("aggregationResults").getJSONObject(0);

      bld.append(groupByResult.getString("function") + TAB);
      JSONArray rowCols = groupByResult.getJSONArray("groupByColumns");
      String[] rowColsArr = new String[rowCols.length()];
      for (int i = 0; i < rowCols.length(); i++) {
        rowColsArr[i] = rowCols.getString(i);
      }
      bld.append(StringUtils.join(rowColsArr, TAB) + NEW_LINE);

      JSONArray groupByResults = groupByResult.getJSONArray("groupByResult");

      for (int i = 0; i < groupByResults.length(); i++) {
        JSONObject entry = groupByResults.getJSONObject(i);
        bld.append(entry.getString("value") + TAB);
        JSONArray colVals = entry.getJSONArray("group");
        String[] colValsArr = new String[colVals.length()];
        for (int j = 0; j < colVals.length(); j++) {
          colValsArr[j] = colVals.getString(j);
        }
        bld.append(StringUtils.join(colValsArr, TAB) + NEW_LINE);
      }
      return bld.toString();
    }
  }

  public boolean execute() throws Exception {
    _quickStartDataDir = new File("quickStartData" + System.currentTimeMillis());
    String quickStartDataDirName = _quickStartDataDir.getName();

    if (!_quickStartDataDir.exists()) {
      _quickStartDataDir.mkdir();
    }

    File schemaFile = new File(quickStartDataDirName + "/baseball.schema");
    File dataFile = new File(quickStartDataDirName + "/baseball.csv");
    File tableCreationJsonFileName = new File(quickStartDataDirName + "/baseballTable.json");

    FileUtils.copyURLToFile(Quickstart.class.getClassLoader().getResource("sample_data/baseball.schema"), schemaFile);
    FileUtils.copyURLToFile(Quickstart.class.getClassLoader().getResource("sample_data/baseball.csv"), dataFile);
    FileUtils.copyURLToFile(Quickstart.class.getClassLoader().getResource("sample_data/baseballTable.json"),
        tableCreationJsonFileName);

    File tempDirOne = new File("/tmp/" + System.currentTimeMillis());
    tempDirOne.mkdir();

    File tempDir = new File("/tmp/" + String.valueOf(System.currentTimeMillis()));
    String tableName = "baseballStats";
    final QuickstartRunner runner =
        new QuickstartRunner(schemaFile, _quickStartDataDir, tempDir, tableName,
            tableCreationJsonFileName.getAbsolutePath());
    runner.clean();
    runner.startAll();
    printStatus(color.CYAN, "Deployed Zookeeper");
    printStatus(color.CYAN, "Deployed controller, broker and server");
    runner.addSchema();
    printStatus(color.CYAN, "Added baseballStats schema");
    runner.addTable();
    printStatus(color.CYAN, "Creating baseballStats table");
    runner.buildSegment();
    printStatus(color.CYAN, "Built index segment for baseballStats");
    runner.pushSegment();
    printStatus(color.CYAN, "Pushing segments to the controller");
    printStatus(color.CYAN, "Waiting for a second for the server to fetch the assigned segment");

    Thread.sleep(5000);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          printStatus(color.GREEN, "***** shutting down offline quick start *****");
          FileUtils.deleteDirectory(_quickStartDataDir);
          runner.clean();
          runner.stop();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });

    printStatus(color.YELLOW, "Offline quickstart complete");

    String q1 = "select count(*) from baseballStats limit 0";
    printStatus(color.YELLOW, "Total number of documents in the table");
    printStatus(color.CYAN, "Query : " + q1);
    printStatus(color.YELLOW, prettyprintResponse(runner.runQuery(q1)));
    printStatus(color.GREEN, "***************************************************");

    String q2 = "select sum('runs') from baseballStats group by playerName top 5 limit 0";
    printStatus(color.YELLOW, "Top 5 run scorers of all time ");
    printStatus(color.CYAN, "Query : " + q2);
    printStatus(color.YELLOW, prettyprintResponse(runner.runQuery(q2)));
    printStatus(color.GREEN, "***************************************************");

    String q3 = "select sum('runs') from baseballStats where yearID=2000 group by playerName top 5 limit 0";
    printStatus(color.YELLOW, "Top 5 run scorers of the year 2000");
    printStatus(color.CYAN, "Query : " + q3);
    printStatus(color.YELLOW, prettyprintResponse(runner.runQuery(q3)));
    printStatus(color.GREEN, "***************************************************");

    String q4 = "select sum('runs') from baseballStats where yearID>=2000 group by playerName limit 0";
    printStatus(color.YELLOW, "Top 10 run scorers after 2000");
    printStatus(color.CYAN, "Query : " + q4);
    printStatus(color.YELLOW, prettyprintResponse(runner.runQuery(q4)));
    printStatus(color.GREEN, "***************************************************");

    String q5 = "select playerName,runs,homeRuns from baseballStats order by yearID limit 10";
    printStatus(color.YELLOW, "Print playerName,runs,homeRuns for 10 records from the table and order them by yearID");
    printStatus(color.CYAN, "Query : " + q5);
    printStatus(color.YELLOW, prettyprintResponse(runner.runQuery(q5)));
    printStatus(color.GREEN, "***************************************************");

    printStatus(color.GREEN,
        "you can always go to http://localhost:9000/query/ to play around in the query console");

    long st = System.currentTimeMillis();
    while (true) {
      if (System.currentTimeMillis() - st >= (60 * 60) * 1000) {
        break;
      }
    }

    printStatus(color.YELLOW, "running since an hour, stopping now");
    return true;
  }

  public static void main(String[] args) throws Exception {
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.ERROR);
    Quickstart st = new Quickstart();
    st.execute();

  }

}
