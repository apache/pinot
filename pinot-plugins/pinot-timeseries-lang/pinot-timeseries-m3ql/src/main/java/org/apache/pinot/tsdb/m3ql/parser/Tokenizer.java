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
package org.apache.pinot.tsdb.m3ql.parser;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;


/**
* TODO: Dummy implementation. Will be switched out with a proper implementation soon.
*/
public class Tokenizer {
  private final String _query;

  public Tokenizer(String query) {
    _query = query;
  }

  public List<List<String>> tokenize() {
    String[] pipelines = _query.split("\\|");
    List<List<String>> result = new ArrayList<>();
    for (String pipeline : pipelines) {
      String command = pipeline.trim().substring(0, pipeline.indexOf("{"));
      if (command.equals("fetch")) {
        result.add(consumeFetch(pipeline.trim()));
      } else {
        result.add(consumeGeneric(pipeline.trim()));
      }
    }
    return result;
  }

  private List<String> consumeFetch(String pipeline) {
    pipeline = pipeline.trim();
    String command = pipeline.substring(0, 5);
    Preconditions.checkState(command.equals("fetch"), "Invalid command: %s", command);
    pipeline = pipeline.substring(5).trim();
    int start = pipeline.indexOf("{");
    int end = pipeline.indexOf("}");
    String args = pipeline.substring(start + 1, end);
    List<String> result = new ArrayList<>();
    result.add("fetch");
    int indexOfEquals = args.indexOf("=");
    while (indexOfEquals != -1) {
      args = args.strip();
      int equalIndex = args.indexOf("=");
      int indexOfQuotes = args.indexOf("\"");
      int lastQuote = indexOfQuotes + 1 + args.substring(indexOfQuotes + 1).indexOf("\"");
      String key = args.substring(0, equalIndex);
      String value = args.substring(indexOfQuotes + 1, lastQuote);
      args = args.substring(lastQuote + 1);
      result.add(key);
      result.add(value);
      if (args.strip().startsWith(",")) {
        args = args.strip().substring(1);
      }
      indexOfEquals = args.indexOf("=");
    }
    return result;
  }

  private List<String> consumeGeneric(String pipeline) {
    List<String> result = new ArrayList<>();
    int indexOfOpenBracket = pipeline.indexOf("{");
    int indexOfClosedBracket = pipeline.indexOf("}");
    result.add(pipeline.substring(0, indexOfOpenBracket));
    String arg = pipeline.substring(indexOfOpenBracket + 1, indexOfClosedBracket);
    if (!arg.isEmpty()) {
      result.add(arg);
    }
    return result;
  }
}
