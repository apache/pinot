package org.apache.pinot.changecheck;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
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

  public static int findDiff(String fileName) throws IOException {
    LineNumberReader br = new LineNumberReader(new FileReader(fileName));
    String firstLineNum = "0";
    String li;
    Pattern funcDef = Pattern.compile("\\s*?\\b(public|private|protected)\\b.*?(.+?)[^{}]*?\\{");
    Pattern annoDef = Pattern.compile("\\s*?@.+?");
    while ((li = br.readLine()) != null) {
      if (li.startsWith("@@")) {
        //this will always get the starting row in the original file
        firstLineNum = li.substring(4, li.indexOf(','));
      }

      if ((li.charAt(0)) == '-') {
        Matcher matcher1 = funcDef.matcher(li.substring(1)); //gets rid of the '-'
        Matcher matcher2 = annoDef.matcher(li.substring(1));
        if (matcher1.matches() || matcher2.matches()) {
          int secondLineNum = br.getLineNumber();
          //return line number of spi change minus the repetitive lines at the top of git diff output
          return Integer.parseInt(firstLineNum) + secondLineNum - 6;
        }
      }
    }
    return -1;
  }

  public static void main(String[] args) throws IOException {
    System.out.println(findDiff(args[0]));
  }
}

