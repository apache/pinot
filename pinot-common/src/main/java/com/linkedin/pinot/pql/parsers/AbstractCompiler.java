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
package com.linkedin.pinot.pql.parsers;

import com.linkedin.pinot.common.request.BrokerRequest;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.json.JSONObject;


public abstract class AbstractCompiler {

  public AbstractCompiler() {
    super();
  }

  public abstract JSONObject compile(String expression) throws RecognitionException;

  public abstract BrokerRequest compileToBrokerRequest(String expression);

  public abstract String getErrorMessage(RecognitionException error);

  protected void printTree(CommonTree ast) {
    print(ast, 0);
  }

  private void print(CommonTree tree, int level) {
    // Indent level
    for (int i = 0; i < level; i++) {
      System.out.print("--");
    }

    if (tree == null) {
      // System.out.println(" null tree.");
      return;
    }

    // Print node description: type code followed by token text
    // System.out.println(" " + tree.getType() + " " + tree.getText());

    // Print all children
    if (tree.getChildren() != null) {
      for (Object ie : tree.getChildren()) {
        print((CommonTree) ie, level + 1);
      }
    }
  }

}
