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
package org.apache.pinot.udf.test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.utils.BytesUtils;


/// A class that generates a markdown report for the UDF test results.
public class UdfReporter {

  private UdfReporter() {
  }

  /// Generates a markdown report for the given UDF and its test results.
  public static void reportAsMarkdown(Udf udf, UdfTestResult.ByScenario byScenario, Writer writer) {
    try (PrintWriter report = new PrintWriter(writer)) {
      report.append("## ").append(udf.getMainFunctionName()).append("\n\n");

      if (udf.getAllFunctionNames().size() > 1) {
        report.append("Other names: ")
            .append(udf.getAllFunctionNames().stream()
                .filter(name -> !name.equals(udf.getMainFunctionName()))
                .collect(Collectors.joining(", ")))
            .append("\n\n");
      }
      
      report.append("### Description\n\n")
          .append(udf.getDescription()).append("\n");
      
      TreeSet<UdfTestScenario> scenarios = new TreeSet<>(Comparator.comparing(UdfTestScenario::getTitle));
      scenarios.addAll(byScenario.getMap().keySet());

      reportSummary(udf, byScenario, scenarios, report);
      reportSignatures(udf, report);
      reportScenarios(udf, byScenario, report, scenarios);
      report.flush();
    }
  }

  private static void reportScenarios(Udf udf, UdfTestResult.ByScenario byScenario, PrintWriter report,
      TreeSet<UdfTestScenario> scenarios) {
    report.append("### Scenarios\n\n");

    for (UdfTestScenario scenario : scenarios) {
      UdfTestResult.BySignature bySignature = byScenario.getMap().get(scenario);

      TreeSet<UdfSignature> signatures = new TreeSet<>(Comparator.comparing(UdfSignature::toString));
      signatures.addAll(bySignature.getMap().keySet());

      report.append("#### ").append(scenario.getTitle()).append("\n\n");

      report.append('\n');

      report.append("| Signature | Call | Expected result | Actual result | Comparison or Error |\n");
      report.append("|-----------|------|-----------------|---------------|---------------------|\n");

      for (UdfSignature signature : signatures) {
        ResultByExample resultByExample = bySignature.getMap().get(signature);

        if (resultByExample instanceof ResultByExample.Partial) {
          ResultByExample.Partial partial = (ResultByExample.Partial) resultByExample;
          for (Map.Entry<UdfExample, UdfExampleResult> exampleEntry : partial.getResultsByExample().entrySet()) {
            UdfExample example = exampleEntry.getKey();
            UdfExampleResult testResult = exampleEntry.getValue();

            // Signature column
            report.append("| ")
                .append(signature.toString()).append(" |");

            // Call column
            report.append(asSqlCallWithLiteralArgs(udf, udf.getMainFunctionName(), example.getInputValues()))
                .append(" |");

            // Expected result
            Object expected = testResult.getExpectedResult();
            Object actual = testResult.getActualResult();

            Function<Object, String> valueFormatter = getResultFormatter(expected, actual);
            report.append(valueFormatter.apply(expected)).append(" |")
                .append(valueFormatter.apply(actual)).append(" |");

            // Comparison or Error
            String error = partial.getErrorsByExample().get(example);
            if (error != null) {
              report.append("❌ ").append(error.replace("\n", " ")).append(" |\n");
            } else {
              UdfTestFramework.EquivalenceLevel comparison = partial.getEquivalenceByExample().get(example);
              report.append(comparison != null ? comparison.name() : "").append(" |\n");
            }
          }
        } else if (resultByExample instanceof ResultByExample.Failure) {
          ResultByExample.Failure failure = (ResultByExample.Failure) resultByExample;

          report.append("| ")
              .append(signature.toString())
              .append(" | - | - | - | ❌ ")
              .append(failure.getErrorMessage().replace("\n", " "))
              .append(" |\n");
        }
      }
      report.append("\n\n");
    }
  }

  private static void reportSignatures(Udf udf, PrintWriter report) {
    Set<UdfSignature> signatures = udf.getExamples().keySet();
    if (!signatures.isEmpty()) {
      report.append("### Signatures\n\n");

      for (UdfSignature signature : signatures) {
        report.append("#### ").append(udf.getMainFunctionName()).append(signature.toString()).append("\n\n");

        String resultDescription = signature.getReturnType().getDescription();
        if (resultDescription != null) {
          report.append(resultDescription).append("\n\n");
        }

        report.append("| Parameter | Type | Description |\n");
        report.append("|-----------|------|-------------|\n");
        for (UdfParameter parameter : signature.getParameters()) {
          report.append("| ")
              .append(parameter.getName()).append(" | ")
              .append(parameter.getDataType().toString().toLowerCase(Locale.US)).append(" | ")
              .append(parameter.getDescription() != null ? parameter.getDescription() : "")
              .append(" |\n");
        }
      }
    }
  }

  private static void reportSummary(Udf udf, UdfTestResult.ByScenario byScenario, TreeSet<UdfTestScenario> scenarios,
      PrintWriter report) {
    SortedMap<UdfTestScenario, String> summaries = new TreeMap<>(Comparator.comparing(UdfTestScenario::getTitle));
    for (UdfTestScenario scenario : scenarios) {
      UdfTestResult.BySignature bySignature = byScenario.getMap().get(scenario);
      summaries.put(scenario, summarize(bySignature));
    }
    report.append("### Summary\n\n");
    if (summaries.values().stream().distinct().count() == 1) {
      String summary = summaries.values().iterator().next();
      if (summary.equals("❌ Unsupported")) {
        report.append("The UDF ").append(udf.getMainFunctionName())
            .append(" is not supported in all scenarios.\n\n");
      } else if (summary.contains("❌")) {
        report.append("The UDF ").append(udf.getMainFunctionName())
            .append(" has failed in all scenarios with the following error: ")
            .append(summary).append("\n\n");
      } else {
        report.append("The UDF ").append(udf.getMainFunctionName())
            .append(" is supported in all scenarios with at least ")
            .append(summary).append(" semantic.\n\n");
      }
    } else {
      report.append("| Scenario | Semantic |\n");
      report.append("|----------|----------|\n");

      for (Map.Entry<UdfTestScenario, String> entry : summaries.entrySet()) {
        report.append("| ").append(entry.getKey().getTitle()).append(" | ")
            .append(entry.getValue()).append(" |\n");
      }
    }
  }

  /// Generates a markdown report for the given UDF test results.
  /// The report is written to the output stream generated by the provided function.
  public static void reportAsMarkdown(UdfTestResult results, Function<Udf, OutputStream> osGenerator)
      throws IOException {

    TreeSet<Udf> udfs = new TreeSet<>(Comparator.comparing(Udf::getMainFunctionName));
    udfs.addAll(results.getResults().keySet());
    for (Udf udf : udfs) {
      try (OutputStream os = osGenerator.apply(udf);
          BufferedWriter writer = new BufferedWriter(new PrintWriter(os))) {
        reportAsMarkdown(udf, results.getResults().get(udf), writer);
      }
    }
  }

  private static String summarize(UdfTestResult.BySignature bySignature) {

    UdfTestFramework.EquivalenceLevel comparison = UdfTestFramework.EquivalenceLevel.EQUAL;
    boolean withSuccess = false;
    int errors = 0;
    for (ResultByExample result : bySignature.getMap().values()) {
      if (result instanceof ResultByExample.Failure) {
        String error = (((ResultByExample.Failure) result)).getErrorMessage().replace("\n", " ");
        return "❌ " + error;
      }
      if (result instanceof ResultByExample.Partial) {
        ResultByExample.Partial partial = (ResultByExample.Partial) result;

        withSuccess |= !partial.getEquivalenceByExample().isEmpty();
        for (UdfTestFramework.EquivalenceLevel value : partial.getEquivalenceByExample().values()) {
          if (value.compareTo(comparison) > 0) {
            comparison = value;
          }
        }

        errors += partial.getErrorsByExample().values().size();
      }
    }

    if (withSuccess) {
      if (errors == 0) {
        return comparison.name();
      }
      return comparison.name() + " with " + errors + " errors.";
    } else {
      return "Not supported";
    }
  }

  private static Function<Object, String> getResultFormatter(Object expected, Object actual) {
    Function<Object, String> valueFormatter;
    if (expected != null && actual != null && expected.getClass().equals(actual.getClass())) {
      valueFormatter = value -> {
        if (value.getClass().isArray()) {
          return Arrays.toString((Object[]) value);
        }
        return value.toString();
      };
    } else {
      valueFormatter = value -> {
        if (value == null) {
          return "NULL";
        } else if (value.getClass().isArray()) {
          return Arrays.toString((Object[]) value)
              + " ( array of " + value.getClass().getComponentType().getSimpleName() + ")";
        } else {
          return value + " (" + value.getClass().getSimpleName() + ")";
        }
      };
    }
    return valueFormatter;
  }

  private static String asSqlCallWithLiteralArgs(Udf udf, String name, List<Object> inputs) {
    List<String> args = inputs.stream().map(o -> {
      if (o == null) {
        return "NULL";
      } else if (o.getClass().isArray()) {
        if (o.getClass().isAssignableFrom(byte[].class)) {
          return "hexToBytes('" + BytesUtils.toHexString((byte[]) o) + "')";
        }
        return Arrays.toString((Object[]) o);
      } else if (o instanceof String) {
        return "'" + o.toString().replace("'", "''") + "'";
      } else {
        return o.toString();
      }
    }).collect(Collectors.toList());
    return udf.asSqlCall(name, args);
  }
}
