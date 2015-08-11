package com.linkedin.thirdeye.reporting;

import io.dropwizard.Configuration;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.*;
import javax.validation.constraints.*;

public class ThirdEyeReportingConfiguration extends Configuration {
  @NotNull
  private String serverUri;
  @NotNull
  private String reportConfigPath; // directory where all report-config.yml files are saved
  @NotNull
  private String dashboardUri;
  @NotNull
  private String reportEmailTemplatePath;

  @JsonProperty
  public String getServerUri() {
    return serverUri;
  }

  public void setServerUri(String serverUri) {
    this.serverUri = serverUri;
  }

  @JsonProperty
  public String getDashboardUri() {
    return dashboardUri;
  }

  public void setDashboardUri(String dashboardUri) {
    this.dashboardUri = dashboardUri;
  }

  @JsonProperty
  public String getReportConfigPath() {
    return reportConfigPath;
  }

  public void setReportConfigPath(String reportConfigPath) {
    this.reportConfigPath = reportConfigPath;
  }

  @JsonProperty
  public String getReportEmailTemplatePath() {
    return reportEmailTemplatePath;
  }

  public void setReportEmailTemplatePath(String reportEmailTemplatePath) {
    this.reportEmailTemplatePath = reportEmailTemplatePath;
  }
}
