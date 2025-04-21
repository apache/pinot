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

package org.apache.pinot; // TODO: fix this

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * method name should not change
 * method should not be deleted
 * public API should not become private
 * method return type should not change
 * method return type annotation should not change
 * arguments should remain the same in count and type
*/

public class GitDiffChecker {

  public static boolean findDiff(String fileName) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(fileName));
    String li;
    Pattern funcDef = Pattern.compile("\\s*?\\b(public|private|protected)\\b.*?(.+?)[^{}]*?\\{");
    Pattern annoDef = Pattern.compile("\\s*?@.+?");
    while ((li = br.readLine()) != null) {
      if ((li.charAt(0)) == '-') {
        Matcher matcher1 = funcDef.matcher(li.substring(1)); //gets rid of the '-'
        Matcher matcher2 = annoDef.matcher(li.substring(1));
        if (matcher1.matches() || matcher2.matches()) {
          return true;
        }
      }
    }
    return false;
  }

  public static void main(String[] args) throws IOException {
    System.out.println(findDiff(args[0]));
  }
}
