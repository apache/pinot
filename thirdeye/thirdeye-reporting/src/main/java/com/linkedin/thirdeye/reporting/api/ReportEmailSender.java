package com.linkedin.thirdeye.reporting.api;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import com.linkedin.thirdeye.reporting.api.TimeRange;

import freemarker.cache.FileTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;

public class ReportEmailSender {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReportEmailSender.class);

  private ReportEmailDataModel reportObjects;
  private String templatePath;

  public ReportEmailSender(ReportEmailDataModel reportObjects, String templatePath) {
    this.reportObjects = reportObjects;
    this.templatePath = templatePath;
  }

  public void emailReport()  {

    try {

      FileTemplateLoader ftl = new FileTemplateLoader(
          new File(templatePath));
      Configuration emailConfiguration = new Configuration();
      emailConfiguration.setTemplateLoader(ftl);
      Template emailReportTemplate = emailConfiguration.getTemplate(reportObjects.getScheduleSpec().getEmailTemplate());

      Writer emailOutput = new StringWriter();
      emailReportTemplate.process(reportObjects, emailOutput);

      Properties props = new Properties();
      props.setProperty(ReportConstants.MAIL_SMTP_HOST_KEY, ReportConstants.MAIL_SMTP_HOST_VALUE);
      Session session = Session.getDefaultInstance(props, null);

      Message emailReportMessage = new MimeMessage(session);
      for (String emailIdFrom : reportObjects.getScheduleSpec().getEmailFrom().split(",")) {
        emailReportMessage.setFrom(new InternetAddress(emailIdFrom, reportObjects.getScheduleSpec().getNameFrom()));
      }
      for (String emailIdTo : reportObjects.getScheduleSpec().getEmailTo().split(",")) {
        emailReportMessage.addRecipient(Message.RecipientType.TO,
                         new InternetAddress(emailIdTo, reportObjects.getScheduleSpec().getNameTo()));
      }
      emailReportMessage.setSubject(ReportConstants.REPORT_SUBJECT_PREFIX +
          " (" + reportObjects.getReportConfig().getCollection().toUpperCase() + ") " +
          reportObjects.getReportConfig().getName());
      emailReportMessage.setContent(emailOutput.toString(), "text/html");
      LOGGER.info("Sending email from {} to {}  ",
          reportObjects.getScheduleSpec().getEmailFrom(), reportObjects.getScheduleSpec().getEmailTo());

      Transport.send(emailReportMessage);

    } catch (Exception e) {
     e.printStackTrace();
    }
  }

  public static void sendErrorReport(List<TimeRange> missingSegments, ScheduleSpec scheduleSpec, ReportConfig reportConfig) {
    StringBuilder sb = new StringBuilder("Data segments missing : ");
    sb.append(System.getProperty("line.separator"));
    for (TimeRange tr : missingSegments) {
      sb.append(ReportConstants.DATE_TIME_FORMATTER.print(tr.getStart()));
      sb.append(" - ");
      sb.append(ReportConstants.DATE_TIME_FORMATTER.print(tr.getEnd()));
      sb.append(System.getProperty("line.separator"));
    }

    String subject = ReportConstants.REPORT_SUBJECT_PREFIX +
    " (Data Segments Missing)" +
    " " + reportConfig.getCollection().toUpperCase() +
    " (" + reportConfig.getEndTimeString() +
    ") " + reportConfig.getName();

    sendErrorReport(subject, sb.toString(), scheduleSpec, reportConfig);
  }

  public static void sendErrorReport(String subject, String message, ScheduleSpec scheduleSpec, ReportConfig reportConfig) {
    try {

      Properties props = new Properties();
      props.setProperty(ReportConstants.MAIL_SMTP_HOST_KEY, ReportConstants.MAIL_SMTP_HOST_VALUE);
      Session session = Session.getDefaultInstance(props, null);

      Message emailReportMessage = new MimeMessage(session);
      for (String emailIdFrom : scheduleSpec.getEmailFrom().split(",")) {
        emailReportMessage.setFrom(new InternetAddress(emailIdFrom, scheduleSpec.getNameFrom()));
      }
      for (String emailIdTo : scheduleSpec.getErrorEmailTo().split(",")) {
        emailReportMessage.addRecipient(Message.RecipientType.TO,
                         new InternetAddress(emailIdTo, scheduleSpec.getNameTo()));
      }
      emailReportMessage.setSubject(subject);
      emailReportMessage.setContent(message, "text/html");
      LOGGER.info("Sending error email from {} to {}  ",
          scheduleSpec.getEmailFrom(), scheduleSpec.getEmailTo());

      Transport.send(emailReportMessage);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
