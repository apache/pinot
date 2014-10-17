package com.linkedin.pinot.pql.parsers;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.json.JSONObject;


public abstract class AbstractCompiler {

  public AbstractCompiler() {
    super();
  }

  public abstract JSONObject compile(String expression) throws RecognitionException;

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
      System.out.println(" null tree.");
      return;
    }

    // Print node description: type code followed by token text
    System.out.println(" " + tree.getType() + " " + tree.getText());

    // Print all children
    if (tree.getChildren() != null) {
      for (Object ie : tree.getChildren()) {
        print((CommonTree) ie, level + 1);
      }
    }
  }

}
