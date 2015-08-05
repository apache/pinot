package com.linkedin.thirdeye.reporting.api;

import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import com.linkedin.thirdeye.anomaly.reporting.AnomalyReportTable;

import freemarker.template.Configuration;
import freemarker.template.Template;

public class ReportEmailSender {

  private List<Table> tables;
  private ScheduleSpec scheduleSpec;
  private ReportConfig reportConfig;
  private List<AnomalyReportTable> anomalyReportTables;

  public ReportEmailSender(List<Table> tables, ScheduleSpec scheduleSpec, ReportConfig reportConfig, List<AnomalyReportTable> anomalyReportTables) {
    this.tables = tables;
    this.scheduleSpec = scheduleSpec;
    this.reportConfig = reportConfig;
    this.anomalyReportTables = anomalyReportTables;
  }

  public void emailReport()  {

    try {
      Configuration emailConfiguration = new Configuration();
      Template emailReportTemplate = emailConfiguration.getTemplate(ReportConstants.REPORT_EMAIL_TEMPLATE_PATH);

      Map<String, Object> rootMap = new HashMap<String, Object>();
      rootMap.put(ReportConstants.REPORT_CONFIG_OBJECT, reportConfig);
      rootMap.put(ReportConstants.TABLES_OBJECT, tables);
      rootMap.put(ReportConstants.ANOMALY_TABLES_OBJECT, anomalyReportTables);

      Writer emailOutput = new StringWriter();
      emailReportTemplate.process(rootMap, emailOutput);
      System.out.println(emailOutput.toString());

      Properties props = new Properties();
      props.setProperty(ReportConstants.MAIL_SMTP_HOST_KEY, ReportConstants.MAIL_SMTP_HOST_VALUE);
      Session session = Session.getDefaultInstance(props, null);

      Message emailReportMessage = new MimeMessage(session);
      for (String emailIdFrom : scheduleSpec.getEmailFrom().split(",")) {
        emailReportMessage.setFrom(new InternetAddress(emailIdFrom, scheduleSpec.getNameFrom()));
      }
      for (String emailIdTo : scheduleSpec.getEmailTo().split(",")) {
        emailReportMessage.addRecipient(Message.RecipientType.TO,
                         new InternetAddress(emailIdTo, scheduleSpec.getNameTo()));
      }
      emailReportMessage.setSubject(reportConfig.getName());
      emailReportMessage.setContent(emailOutput.toString(), "text/html");
      Transport.send(emailReportMessage);
    } catch (Exception e) {
     e.printStackTrace();
    }
  }

}
