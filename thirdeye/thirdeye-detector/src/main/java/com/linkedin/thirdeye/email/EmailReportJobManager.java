package com.linkedin.thirdeye.email;

import com.linkedin.thirdeye.ThirdEyeDetectorConfiguration;
import com.linkedin.thirdeye.api.EmailConfiguration;
import com.linkedin.thirdeye.db.AnomalyResultDAO;
import com.linkedin.thirdeye.db.EmailConfigurationDAO;
import com.linkedin.thirdeye.driver.AnomalyDetectionJob;
import io.dropwizard.lifecycle.Managed;
import org.hibernate.SessionFactory;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EmailReportJobManager implements Managed {
  private static final Logger LOG = LoggerFactory.getLogger(EmailReportJobManager.class);
  private final Scheduler quartzScheduler;
  private final EmailConfigurationDAO configurationDAO;
  private final Map<String, EmailConfiguration> jobKeys;
  private final AnomalyResultDAO resultDAO;
  private final SessionFactory sessionFactory;
  private final Object sync;

  public EmailReportJobManager(Scheduler quartzScheduler,
                               EmailConfigurationDAO configurationDAO,
                               AnomalyResultDAO resultDAO,
                               SessionFactory sessionFactory) {
    this.quartzScheduler = quartzScheduler;
    this.configurationDAO = configurationDAO;
    this.sessionFactory = sessionFactory;
    this.resultDAO = resultDAO;
    this.jobKeys = new HashMap<>();
    this.sync = new Object();
  }

  @Override
  public void start() throws Exception {
    synchronized (sync) {
      List<EmailConfiguration> emailConfigs = configurationDAO.findAll();
      int i = 0;
      for (EmailConfiguration emailConfig : emailConfigs) {
        String triggerKey = String.format("email_trigger_%d", i);
        CronTrigger trigger = TriggerBuilder.newTrigger()
            .withIdentity(triggerKey)
            .withSchedule(CronScheduleBuilder.cronSchedule(emailConfig.getCron()))
            .build();

        String jobKey = String.format("email_job_%d", i);
        JobDetail job = JobBuilder.newJob(EmailReportJob.class)
            .withIdentity(jobKey)
            .build();

        job.getJobDataMap().put(EmailReportJob.RESULT_DAO, resultDAO);
        job.getJobDataMap().put(EmailReportJob.CONFIG, emailConfig);
        job.getJobDataMap().put(EmailReportJob.SESSION_FACTORY, sessionFactory);

        jobKeys.put(jobKey, emailConfig);
        quartzScheduler.scheduleJob(job, trigger);
        LOG.info("Started {}: {}", jobKey, emailConfig);

        i++;
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
