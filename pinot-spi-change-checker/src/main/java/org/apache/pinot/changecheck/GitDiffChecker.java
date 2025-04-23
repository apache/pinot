package org.apache.pinot.changecheck;

import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
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

  public static String findDiff(String fileName) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(fileName));
    String li;
    Pattern funcDef = Pattern.compile("^\\s*?.+?(.*?)[^{}]*?[{|;]");
    Pattern annoDef = Pattern.compile("^\\s*?@\\S+?\\n");
    while ((li = br.readLine()) != null) {
      if ((!li.isEmpty()) && (li.charAt(0) == '-') && (!li.startsWith("---"))) {
        Matcher matcher1 = funcDef.matcher(li.substring(1)); //gets rid of the '-' at the beginning
        Matcher matcher2 = annoDef.matcher(li.substring(1));
        if (matcher1.matches() || matcher2.matches()) {
          return li.substring(1).trim();
        }
      }
    }
    return "0";
  }

  public static void main(String[] args) throws IOException {
    System.out.println(findDiff(args[0]));
  }
}