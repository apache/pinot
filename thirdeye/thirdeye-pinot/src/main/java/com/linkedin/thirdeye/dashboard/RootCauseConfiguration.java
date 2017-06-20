package com.linkedin.thirdeye.dashboard;

import java.util.Collections;
import java.util.List;


public class RootCauseConfiguration {
  int parallelism = 1;
  String definitionsPath = "rca.yml";
  List<String> formatters = Collections.emptyList();

  public int getParallelism() {
    return parallelism;
  }

  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  public String getDefinitionsPath() {
    return definitionsPath;
  }

  public void setDefinitionsPath(String definitionsPath) {
    this.definitionsPath = definitionsPath;
  }

  public List<String> getFormatters() {
    return formatters;
  }

  public void setFormatters(List<String> formatters) {
    this.formatters = formatters;
  }

}
