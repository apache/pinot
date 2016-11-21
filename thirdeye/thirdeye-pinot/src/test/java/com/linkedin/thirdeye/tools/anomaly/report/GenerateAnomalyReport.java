package com.linkedin.thirdeye.tools.anomaly.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import java.io.File;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GenerateAnomalyReport {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
  private static final NumberFormat nf = NumberFormat.getInstance();

  MergedAnomalyResultManager anomalyResultManager;
  Date startTime;
  Date endTime;
  List<String> collections;
  String dashboardUrl;

  public GenerateAnomalyReport(Date startTime, Date endTime, File persistenceConfig,
      List<String> datasets, String dashboardUrl) {
    DaoProviderUtil.init(persistenceConfig);
    anomalyResultManager = DaoProviderUtil.getInstance(MergedAnomalyResultManagerImpl.class);
    this.startTime = startTime;
    this.endTime = endTime;
    this.collections = datasets;
    this.dashboardUrl = dashboardUrl;

    System.out.println(
        "building report : " + startTime + " -- " + endTime + ", for collections " + datasets);
  }

  void buildReport() {
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    for (String collection : collections) {
      anomalies.addAll(anomalyResultManager
          .findByCollectionTime(collection, startTime.getTime(), endTime.getTime(), false));
    }

    List<AnomalyReportDTO> anomalyReportDTOList = new ArrayList<>();
    System.out.println(anomalies);
    Set<String> metrics = new HashSet<>();
    int alertedAnomalies = 0;
    int feedbackCollected = 0;
    int trueAlert = 0;
    int falseAlert = 0;
    int nonActionable = 0;

    for (MergedAnomalyResultDTO anomaly : anomalies) {
      metrics.add(anomaly.getMetric());
      if (anomaly.isNotified()) {
        alertedAnomalies++;
      }
      if (anomaly.getFeedback() != null) {
        feedbackCollected++;
        if (anomaly.getFeedback().getFeedbackType().equals(AnomalyFeedbackType.ANOMALY)) {
          trueAlert++;
        } else if (anomaly.getFeedback().getFeedbackType()
            .equals(AnomalyFeedbackType.NOT_ANOMALY)) {
          falseAlert++;
        } else {
          nonActionable++;
        }
      }
      String feedbackVal = getFeedback(
          anomaly.getFeedback() == null ? "NA" : anomaly.getFeedback().getFeedbackType().name());

      AnomalyReportDTO anomalyReportDTO =
          new AnomalyReportDTO(anomaly.getId(), feedbackVal, nf.format(anomaly.getWeight()),
              anomaly.getMetric(), df.format(new Date(anomaly.getStartTime())),
              nf.format(getTimeDiffInHours(anomaly.getStartTime(), anomaly.getEndTime())),
              getAnomalyURL(anomaly));

      anomalyReportDTOList.add(anomalyReportDTO);
    }

    System.out.println("#Anomalies : " + anomalies.size());
    System.out.println("Total metrics : " + metrics.size());
    System.out.println("Alert : " + alertedAnomalies);
    System.out.println("Feedback : " + feedbackCollected);
    System.out.println("TrueAlert: " + trueAlert);
    System.out.println("FalseAlert: " + falseAlert);
    System.out.println("Non Actionable:" + nonActionable);

    System.out.println(anomalyReportDTOList);
  }

  double getTimeDiffInHours(long start, long end) {
    return Double.valueOf((end - start) / 1000) / 3600;
  }

  String getFeedback(String feedbackType) {
    switch (feedbackType) {
    case "ANOMALY":
      return "Confirmed Anomaly";
    case "NOT_ANOMALY":
      return "False Alarm";
    case "ANOMALY_NO_ACTION":
      return "NotActionable";
    }
    return "NA";
  }

  String getAnomalyURL(MergedAnomalyResultDTO anomalyResultDTO) {
    String urlPart =
        "#view=anomalies&dataset=%s&metrics=%s&currentStart=%s&currentEnd=%s";
    return dashboardUrl + String
        .format(urlPart, anomalyResultDTO.getCollection(), anomalyResultDTO.getMetric(),
            anomalyResultDTO.getStartTime(), anomalyResultDTO.getEndTime());
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.out.println("please pass report config path\n");
      System.exit(1);
    }
    File configFile = new File(args[0]);
    AnomalyReportConfig config = OBJECT_MAPPER.readValue(configFile, AnomalyReportConfig.class);

    File persistenceFile = new File(config.getPersistenceConfigPath());
    if (!persistenceFile.exists()) {
      System.err.println("Missing file:" + persistenceFile);
      System.exit(1);
    }
    GenerateAnomalyReport reportGenerator =
        new GenerateAnomalyReport(df.parse(config.getStartTimeIso()),
            df.parse(config.getEndTimeIso()), persistenceFile,
            Arrays.asList(config.getDatasets().split(",")), config.getTeBaseUrl());

    reportGenerator.buildReport();
    System.exit(-1);
  }

  static class AnomalyReportDTO {
    String metric;
    String startDateTime;
    String windowSize;
    String lift;
    String feedback;
    long anomalyId;
    String anomalyURL;

    public AnomalyReportDTO(long anomalyId, String feedback, String lift, String metric,
        String startDateTime, String windowSize, String anomalyURL) {
      this.anomalyId = anomalyId;
      this.feedback = feedback;
      this.lift = lift;
      this.metric = metric;
      this.startDateTime = startDateTime;
      this.windowSize = windowSize;
      this.anomalyURL = anomalyURL;
    }

    public long getAnomalyId() {
      return anomalyId;
    }

    public void setAnomalyId(long anomalyId) {
      this.anomalyId = anomalyId;
    }

    public String getFeedback() {
      return feedback;
    }

    public void setFeedback(String feedback) {
      this.feedback = feedback;
    }

    public String getLift() {
      return lift;
    }

    public void setLift(String lift) {
      this.lift = lift;
    }

    public String getMetric() {
      return metric;
    }

    public void setMetric(String metric) {
      this.metric = metric;
    }

    public String getStartDateTime() {
      return startDateTime;
    }

    public void setStartDateTime(String startDateTime) {
      this.startDateTime = startDateTime;
    }

    public String getWindowSize() {
      return windowSize;
    }

    public void setWindowSize(String windowSize) {
      this.windowSize = windowSize;
    }

    public String getAnomalyURL() {
      return anomalyURL;
    }

    public void setAnomalyURL(String anomalyURL) {
      this.anomalyURL = anomalyURL;
    }

    @Override public String toString() {
      return "AnomalyReportDTO{" +
          "anomalyId=" + anomalyId +
          ", metric='" + metric + '\'' +
          ", startDateTime='" + startDateTime + '\'' +
          ", windowSize='" + windowSize + '\'' +
          ", lift='" + lift + '\'' +
          ", feedback='" + feedback + '\'' +
          ", anomalyURL='" + anomalyURL + '\'' +
          '}';
    }
  }
}
