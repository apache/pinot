package com.linkedin.thirdeye.email;

import com.linkedin.thirdeye.api.EmailConfiguration;
import com.linkedin.thirdeye.db.AnomalyResultDAO;
import com.linkedin.thirdeye.db.EmailConfigurationDAO;
import io.dropwizard.lifecycle.Managed;
import org.hibernate.SessionFactory;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class EmailReportJobManager implements Managed {
  private static final Logger LOG = LoggerFactory.getLogger(EmailReportJobManager.class);
  private final Scheduler quartzScheduler;
  private final EmailConfigurationDAO configurationDAO;
  private final Map<String, EmailConfiguration> jobKeys;
  private final AnomalyResultDAO resultDAO;
  private final SessionFactory sessionFactory;
  private final AtomicInteger applicationPort;
  private final Object sync;

  public EmailReportJobManager(Scheduler quartzScheduler,
                               EmailConfigurationDAO configurationDAO,
                               AnomalyResultDAO resultDAO,
                               SessionFactory sessionFactory,
                               AtomicInteger applicationPort) {
    this.quartzScheduler = quartzScheduler;
    this.configurationDAO = configurationDAO;
    this.sessionFactory = sessionFactory;
    this.resultDAO = resultDAO;
    this.applicationPort = applicationPort;
    this.jobKeys = new HashMap<>();
    this.sync = new Object();
  }

  public void sendAdHoc(Long id) throws Exception {
    synchronized (sync) {
      EmailConfiguration emailConfig = configurationDAO.findById(id);

      String triggerKey = String.format("ad_hoc_email_trigger_%d", emailConfig.getId());
      Trigger trigger = TriggerBuilder.newTrigger()
          .withIdentity(triggerKey)
          .startNow()
          .build();

      String jobKey = String.format("ad_hoc_email_job_%d", emailConfig.getId());
      JobDetail job = JobBuilder.newJob(EmailReportJob.class)
          .withIdentity(jobKey)
          .build();

      job.getJobDataMap().put(EmailReportJob.RESULT_DAO, resultDAO);
      job.getJobDataMap().put(EmailReportJob.CONFIG, emailConfig);
      job.getJobDataMap().put(EmailReportJob.SESSION_FACTORY, sessionFactory);
      job.getJobDataMap().put(EmailReportJob.APPLICATION_PORT, applicationPort.get());

      quartzScheduler.scheduleJob(job, trigger);
      LOG.info("Started {}: {}", jobKey, emailConfig);
    }
  }

  @Override
  public void start() throws Exception {
    synchronized (sync) {
      List<EmailConfiguration> emailConfigs = configurationDAO.findAll();
      for (EmailConfiguration emailConfig : emailConfigs) {
        if (emailConfig.getIsActive()) {
          String triggerKey = String.format("email_trigger_%d", emailConfig.getId());
          CronTrigger trigger = TriggerBuilder.newTrigger()
              .withIdentity(triggerKey)
              .withSchedule(CronScheduleBuilder.cronSchedule(emailConfig.getCron()))
              .build();

          String jobKey = String.format("email_job_%d", emailConfig.getId());
          JobDetail job = JobBuilder.newJob(EmailReportJob.class)
              .withIdentity(jobKey)
              .build();

          job.getJobDataMap().put(EmailReportJob.RESULT_DAO, resultDAO);
          job.getJobDataMap().put(EmailReportJob.CONFIG, emailConfig);
          job.getJobDataMap().put(EmailReportJob.SESSION_FACTORY, sessionFactory);
          job.getJobDataMap().put(EmailReportJob.APPLICATION_PORT, applicationPort.get());

          jobKeys.put(jobKey, emailConfig);
          quartzScheduler.scheduleJob(job, trigger);
          LOG.info("Started {}: {}", jobKey, emailConfig);
        }
      }
    }
  }

  @Override
  public void stop() throws Exception {
    synchronized (sync) {
      for (Map.Entry<String, EmailConfiguration> entry : jobKeys.entrySet()) {
        quartzScheduler.deleteJob(JobKey.jobKey(entry.getKey()));
        LOG.info("Stopped {}: {}", entry.getKey(), entry.getValue());
      }
    }
  }

  public void reset() throws Exception {
    synchronized (sync) {
      stop();
      start();
    }
  }
}
