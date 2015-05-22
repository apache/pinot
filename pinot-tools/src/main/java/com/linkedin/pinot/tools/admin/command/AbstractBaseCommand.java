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
package com.linkedin.pinot.tools.admin.command;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLConnection;

import org.json.JSONException;
import org.kohsuke.args4j.Option;


/**
 * Super class for all the commands.
 * Implements the common printUsage method.
 *
 * @author Mayank Shrivastava <mshrivastava@linkedin.com>
 */
public class AbstractBaseCommand {
  public AbstractBaseCommand() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        cleanup();
      }
    });
  }

  public void cleanup() {
  };

  public String getName() {
    return "BaseCommand";
  }

  public void printUsage() {
    System.out.println("Usage: " + this.getName());

    for (Field f : this.getClass().getDeclaredFields()) {
      if (f.isAnnotationPresent(Option.class)) {
        Option option = f.getAnnotation(Option.class);

        System.out.println(String.format("\t%-15s %-15s: %s (reqiured=%s)", option.name(), option.metaVar(),
            option.usage(), option.required()));
      }
    }
  }

  public static int getPID() {
    String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
    return Integer.parseInt(processName.split("@")[0]);
  }

  protected void savePID(String fileName) throws IOException {
    FileWriter pidFile = new FileWriter(fileName);
    pidFile.write(getPID());
    pidFile.close();
  }

  public static String sendPostRequest(String urlString, String payload) throws UnsupportedEncodingException,
      IOException, JSONException {
    final URL url = new URL(urlString);
    final URLConnection conn = url.openConnection();

    conn.setDoOutput(true);
    final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), "UTF-8"));

    writer.write(payload, 0, payload.length());
    writer.flush();

    final BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
    final StringBuilder sb = new StringBuilder();
    String line = null;

    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }

    return sb.toString();
  }
}
