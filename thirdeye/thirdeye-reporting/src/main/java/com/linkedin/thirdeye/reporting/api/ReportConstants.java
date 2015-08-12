package com.linkedin.thirdeye.reporting.api;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public final class ReportConstants {

  public static String YAML_FILE_SUFFIX = ".yml";

  public static String CONFIG_FILE_KEY = "CONFIG_FILE_KEY";
  public static String SERVER_URI_KEY = "SERVER_URI_KEY";
  public static String DASHBOARD_URI_KEY = "DASHBOARD_URI_KEY";
  public static String TEMPLATE_PATH_KEY = "TEMPLATE_PATH_KEY";
  public static String QUERY_EXECUTOR = "QUERY_EXECUTOR_KEY";

  public static String REPORT_CONFIG_OBJECT = "reportConfig";
  public static String TABLES_OBJECT = "tables";
  public static String ANOMALY_TABLES_OBJECT = "anomalyTables";
  public static String SCHEDULE_SPEC_OBJECT = "scheduleSpec";

  public static String MAIL_SMTP_HOST_KEY = "mail.smtp.host";
  public static String MAIL_SMTP_HOST_VALUE = "email.corp.linkedin.com";
  public static String REPORT_EMAIL_TEMPLATE_PATH = "src/main/resources/report-email-template.ftl";

  public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("YYYY-MM-dd h a z");
  public static final DateTimeFormatter HOUR_FORMATTER = DateTimeFormat.forPattern("h a");

  public static String REPORT_SUBJECT_PREFIX = "Thirdeye Reports";


}
