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

import java.awt.Desktop;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.tools.admin.command.AbstractBaseCommand;
import com.linkedin.pinot.tools.admin.command.Command;
import com.linkedin.pinot.tools.admin.command.QuickstartRunner;


public class Quickstart {
  private static Logger LOGGER = LoggerFactory.getLogger(Quickstart.class); 

  public Quickstart() {

  }

  public boolean execute() throws Exception {
    File dataFile =
        new File(Quickstart.class.getClassLoader().getResource("sample_data/baseball.csv").toExternalForm());

    File tempDirOne = new File("/tmp/" + System.currentTimeMillis());
    tempDirOne.mkdir();

    if (true) {
      InputStream s = Quickstart.class.getClassLoader().getResource("sample_data/baseball.csv").openStream();
      IOUtils.copy(s, new FileOutputStream(new File(tempDirOne, "baseball.csv")));
      dataFile = new File(tempDirOne, "baseball.csv");
    }

    File schemaFile =
        new File(Quickstart.class.getClassLoader().getResource("sample_data/baseball.schema").toExternalForm());

    if (true) {
      InputStream s = Quickstart.class.getClassLoader().getResource("sample_data/baseball.schema").openStream();
      IOUtils.copy(s, new FileOutputStream(new File(tempDirOne, "baseball.schema")));
      schemaFile = new File(tempDirOne, "baseball.schema");
    }

    System.out.println("schema file : " + schemaFile.getAbsolutePath());
    System.out.println("data file : " + dataFile.getAbsolutePath());
    File tempDir = new File("/tmp/" + String.valueOf(System.currentTimeMillis()));
    String tableName = "baseballStats";
    final QuickstartRunner runner = new QuickstartRunner(schemaFile, dataFile, tempDir, tableName);
    runner.clean();
    System.out.println("**************************** : starting all");
    runner.startAll();
    System.out.println("**************************** : started all");
    runner.addSchema();
    System.out.println("**************************** : schema added");
    runner.addTable();
    System.out.println("**************************** : tabled added");
    runner.buildSegment();
    System.out.println("**************************** : segment build");
    runner.pushSegment();
    System.out.println("**************************** : segment pushed");

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          runner.clean();
          runner.stop();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });

    Desktop desktop = Desktop.isDesktopSupported() ? Desktop.getDesktop() : null;
    if (desktop != null && desktop.isSupported(Desktop.Action.BROWSE)) {
      try {
        desktop.browse(new URI("http://localhost:9000/query/"));
      } catch (Exception e) {
        e.printStackTrace();
        return false;
      }
    }

    System.out.println("quickstart setup complete");
    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
      String line;

      while ((line = br.readLine()) != null) {
        try {
          if (line.startsWith("run")) {
            String query = line.substring(line.indexOf("run") + "run".length(), line.length());
            System.out.println("running query : " + query);
            System.out.println(runner.runQuery(query).toString(1));
            continue;
          }
          runner.printUsageAndInfo();
        } catch (Exception e) {
          System.out.println("cannot understand command : " + line + " : only commands understood is : run <QUERY> ");
          return false;
        }

      }

    } catch (IOException io) {
      io.printStackTrace();
      return false;
    }
    return false;
  }
 
  public static void main(String[] args) throws Exception {
     org.apache.log4j.Logger.getRootLogger().setLevel(Level.ERROR); 
    Quickstart st = new Quickstart();
    st.execute();

  }

}
