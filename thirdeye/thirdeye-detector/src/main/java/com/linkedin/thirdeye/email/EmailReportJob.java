package com.linkedin.thirdeye.email;

import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.ThirdEyeDetectorConfiguration;
import com.linkedin.thirdeye.api.AnomalyResult;
import com.linkedin.thirdeye.api.EmailConfiguration;
import com.linkedin.thirdeye.db.AnomalyResultDAO;
import com.linkedin.thirdeye.db.HibernateSessionWrapper;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.HtmlEmail;
import org.apache.commons.mail.SimpleEmail;
import org.hibernate.SessionFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class EmailReportJob implements Job {
  private static final Logger LOG = LoggerFactory.getLogger(EmailReportJob.class);

  public static final String CONFIG = "CONFIG";
  public static final String RESULT_DAO = "RESULT_DAO";
  public static final String SESSION_FACTORY = "SESSION_FACTORY";
  public static final String CHARSET = "UTF-8";

  @Override
  public void execute(final JobExecutionContext context) throws JobExecutionException {
    final EmailConfiguration config = (EmailConfiguration) context.getJobDetail().getJobDataMap().get(CONFIG);
    SessionFactory sessionFactory = (SessionFactory) context.getJobDetail().getJobDataMap().get(SESSION_FACTORY);

    // Get time
    long deltaMillis = TimeUnit.MILLISECONDS.convert(config.getWindowSize(), config.getWindowUnit());
    final DateTime now = DateTime.now().toDateTime(DateTimeZone.UTC);
    final DateTime then = now.minus(deltaMillis);

    // Get the anomalies in that range
    final List<AnomalyResult> results;
    try {
      results = new HibernateSessionWrapper<List<AnomalyResult>>(sessionFactory).execute(new Callable<List<AnomalyResult>>() {
        @Override
        public List<AnomalyResult> call() throws Exception {
          AnomalyResultDAO resultDAO = (AnomalyResultDAO) context.getJobDetail().getJobDataMap().get(RESULT_DAO);
          return resultDAO.findAllByCollectionAndTime(config.getCollection(), then, now);
        }
      });
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }

    // Render template
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (Writer out = new OutputStreamWriter(baos, CHARSET)) {
      Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_21);
      freemarkerConfig.setClassForTemplateLoading(getClass(), "/email/");
      freemarkerConfig.setDefaultEncoding(CHARSET);
      freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
      Map<String, Object> templateData = ImmutableMap.of(
          "anomalyResults", (Object) results,
          "startTime", then,
          "endTime", now
      );
      Template template = freemarkerConfig.getTemplate("simple-anomaly-report.ftl");
      template.process(templateData, out);
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }

    // Send email
    HtmlEmail email;
    try {
      email = new HtmlEmail();
      email.setHostName(config.getSmtpHost());
      email.setSmtpPort(config.getSmtpPort());
      if (config.getSmtpUser() != null && config.getSmtpPassword() != null) {
        email.setAuthenticator(new DefaultAuthenticator(config.getSmtpUser(), config.getSmtpPassword()));
        email.setSSLOnConnect(true);
      }
      email.setFrom(config.getFromAddress());
      email.addTo(config.getToAddress());
      email.setSubject(String.format("[ThirdEye] (%s) %d anomalies (%s to %s)",
          config.getCollection(), results.size(), then, now));
      email.setHtmlMsg(new String(baos.toByteArray(), CHARSET));
      email.send();
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }

    LOG.info("Sent email! {}", config);
  }
}
