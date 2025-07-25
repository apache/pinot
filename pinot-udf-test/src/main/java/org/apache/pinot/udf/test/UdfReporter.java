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

import java.io.PrintWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collection;
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
import javax.annotation.Nullable;
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
      report.append("## ").append(udf.getMainName()).append("\n\n");

      if (udf.getAllNames().size() > 1) {
        report.append("Other names: ")
            .append(udf.getAllNames().stream()
                .filter(name -> !name.equals(udf.getMainName()))
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

    // This is used to create a collapsed section in the markdown report
    report.append("<details>\n"
        + "\n"
        + "<summary>Click to open</summary>\n\n");

    for (UdfTestScenario scenario : scenarios) {
      UdfTestResult.BySignature bySignature = byScenario.getMap().get(scenario);

      TreeSet<UdfSignature> signatures = new TreeSet<>(Comparator.comparing(UdfSignature::toString));
      signatures.addAll(bySignature.getMap().keySet());

      report.append("#### ").append(scenario.getTitle()).append("\n\n");

      report.append('\n');

      report.append("| Example | Call | Expected result | Actual result | Report |\n");
      report.append("|---------|------|-----------------|---------------|--------|\n");

      for (UdfSignature signature : signatures) {
        ResultByExample resultByExample = bySignature.getMap().get(signature);

        if (resultByExample instanceof ResultByExample.Partial) {
          ResultByExample.Partial partial = (ResultByExample.Partial) resultByExample;
          Set<Map.Entry<UdfExample, UdfExampleResult>> entries = new TreeSet<>(
              Comparator.comparing(entry -> entry.getKey().getId()));
          entries.addAll(partial.getResultsByExample().entrySet());
          for (Map.Entry<UdfExample, UdfExampleResult> exampleEntry : entries) {
            UdfExample example = exampleEntry.getKey();
            UdfExampleResult testResult = exampleEntry.getValue();

            // Signature column
            report.append("| ")
                .append(example.getId()).append(" | ");

            // Call column
            report.append(asSqlCallWithLiteralArgs(udf, udf.getMainName(), example.getInputValues()))
                .append(" | ");

            // Expected result
            Object expected = testResult.getExpectedResult();
            Object actual = testResult.getActualResult();

            Function<Object, String> valueFormatter = getResultFormatter(expected, actual);
            report.append(valueFormatter.apply(expected)).append(" | ")
                .append(valueFormatter.apply(actual)).append(" | ");

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
      report.append("\n");
    }
    // Close the collapsed section
    report.append("\n</details>\n\n");
  }

  private static void reportSignatures(Udf udf, PrintWriter report) {
    Set<UdfSignature> signatures = new TreeSet<>(Comparator.comparing(UdfSignature::toString));
    signatures.addAll(udf.getExamples().keySet());
    if (!signatures.isEmpty()) {
      report.append("### Signatures\n\n");

      boolean paramsAlreadyPrinted = false;
      for (UdfSignature signature : signatures) {
        report.append("#### ").append(udf.getMainName()).append(signature.toString()).append("\n\n");

        String resultDescription = signature.getReturnType().getDescription();
        if (resultDescription != null) {
          report.append(resultDescription).append("\n\n");
        }

        if (signature.getParameters().stream().anyMatch(p -> p.getDescription() != null)) {
          // This is used to create a collapsed section in the markdown report
          if (paramsAlreadyPrinted) {
            report.append("<details>\n"
                + "\n"
                + "<summary>Click to open</summary>\n\n");
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
          if (paramsAlreadyPrinted) {
            // Close the collapsed section
            report.append("\n</details>\n\n");
          } else {
            paramsAlreadyPrinted = true;
          }
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

    udf.getExamples().keySet().stream()
        .min(Comparator.comparing(UdfSignature::toString))
        .ifPresent(udfSignature -> {

          report.append("|Call | Result (with null handling) | Result (without null handling)\n");
          report.append("|-----|-----------------------------|------------------------------|\n");

          for (UdfExample example : udf.getExamples().get(udfSignature)) {
            // Expected result
            Object withNull = example.getResult(UdfExample.NullHandling.ENABLED);
            Object withoutNull = example.getResult(UdfExample.NullHandling.DISABLED);

            // Call column
            report.append("| ")
                .append(asSqlCallWithLiteralArgs(udf, udf.getMainName(), example.getInputValues()))
                .append(" | ")
                .append(valueToString(withNull))
                .append(" | ")
                .append(valueToString(withoutNull))
                .append(" |\n");
          }
          report.append("\n");
        });

    if (summaries.values().stream().distinct().count() == 1) {
      String summary = summaries.values().iterator().next();
      if (summary.equals("❌ Unsupported")) {
        report.append("The UDF ").append(udf.getMainName())
            .append(" is not supported in all scenarios.\n\n");
      } else if (summary.contains("❌")) {
        report.append("The UDF ").append(udf.getMainName())
            .append(" has failed in all scenarios with the following error: ")
            .append(summary).append("\n\n");
      } else if (summary.equals("EQUAL")) {
        report.append("The UDF ").append(udf.getMainName())
            .append(" is supported in all scenarios\n\n");
      } else {
        report.append("The UDF ").append(udf.getMainName())
            .append(" is supported in all scenarios with at least ")
            .append(summary).append(" semantic.\n\n");
      }
    } else {
      report.append("This UDF has different semantics in different scenarios:\n\n");

      report.append("| Scenario | Semantic |\n");
      report.append("|----------|----------|\n");

      for (Map.Entry<UdfTestScenario, String> entry : summaries.entrySet()) {
        report.append("| ").append(entry.getKey().getTitle()).append(" | ")
            .append(entry.getValue()).append(" |\n");
      }
    }
  }

  private static String valueToString(@Nullable Object value) {
    if (value == null) {
      return "NULL";
    } else if (value.getClass().isArray()) {
      if (value.getClass().isAssignableFrom(byte[].class)) {
        return "hexToBytes('" + BytesUtils.toHexString((byte[]) value) + "')";
      }
      return Arrays.stream((Object[]) value)
          .map(UdfReporter::valueToString)
          .collect(Collectors.joining(", ", "[", "]"));
    } else if (value instanceof String) {
      return "'" + value.toString().replace("'", "''") + "'";
    } else if (value instanceof Collection) {
      return ((Collection<?>) value).stream()
          .map(UdfReporter::valueToString)
          .collect(Collectors.joining(", ", "[", "]"));
    } else if (value instanceof Number || value instanceof Boolean) {
      return value.toString();
    } else {
      return value.toString();
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
          String componentTypeName = value.getClass().getComponentType().getSimpleName();
          String valueDesc;
          switch (componentTypeName) {
            case "int":
              valueDesc = Arrays.toString((int[]) value);
              break;
            case "long":
              valueDesc = Arrays.toString((long[]) value);
              break;
            case "float":
              valueDesc = Arrays.toString((float[]) value);
              break;
            case "double":
              valueDesc = Arrays.toString((double[]) value);
              break;
            default:
              valueDesc = Arrays.toString((Object[]) value);
              break;
          }
          return valueDesc + " ( array of " + componentTypeName + ")";
        } else {
          return value + " (" + value.getClass().getSimpleName() + ")";
        }
      };
    }
    return valueFormatter;
  }

  private static String asSqlCallWithLiteralArgs(Udf udf, String name, List<Object> inputs) {
    List<String> args = inputs.stream()
        .map(UdfReporter::valueToString)
        .collect(Collectors.toList());
    return udf.asSqlCall(name, args);
  }
}
