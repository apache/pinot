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
package com.linkedin.pinot.dont.check.it.in;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import org.apache.commons.lang.StringUtils;

/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Oct 27, 2014
 */

public class BQlQueryLog {

  InputStream    fis;
  BufferedReader br;
  String         line;

  public BQlQueryLog(File file) throws FileNotFoundException {
    fis = new FileInputStream(new File("/home/dpatel/experiments/data/out_utc"));
    br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
  }

  public void analyze() throws IOException {
    final FileWriter fw = new FileWriter("/home/dpatel/experiments/data/lva1_queries_un_encoded");

    while ((line = br.readLine()) != null) {
      final String bql = (line.split("BQL:")[1]).split("Time")[0];
      fw.write(StringUtils.trim(bql));
      fw.write("\n");
    }

    br.close();
    fis.close();
    fw.close();
  }

  public static void main(String[] args) throws IOException {
    final BQlQueryLog log = new BQlQueryLog(null);
    log.analyze();
  }
}
