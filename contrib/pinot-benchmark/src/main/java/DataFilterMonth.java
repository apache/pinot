/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;


// Filter the data file into several segments according to the l_shipdate month
public final class DataFilterMonth {
  private DataFilterMonth() {
  }

  public static void main(String[] args) throws Exception {
    File dir = new File("output");
    if (!dir.exists()) {
      dir.mkdirs();
    }
    Map<String, BufferedWriter> map = new HashMap<>();
    try (BufferedReader reader = new BufferedReader(new FileReader("input/lineitem.tbl"))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String month = line.split("\\|")[10].substring(0, 7);
        BufferedWriter writer = map.get(month);
        if (writer == null) {
          writer = new BufferedWriter(new FileWriter("output/" + month + ".csv"));
          map.put(month, writer);
        }
        writer.write(line);
        writer.newLine();
      }
      for (String month : map.keySet()) {
        map.get(month).close();
      }
    } finally {
      for (BufferedWriter writer : map.values()) {
        if (writer != null) {
          writer.close();
        }
      }
    }
  }
}
