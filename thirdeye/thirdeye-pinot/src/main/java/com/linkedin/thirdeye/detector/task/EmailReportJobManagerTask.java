package com.linkedin.thirdeye.detector.task;

import java.io.PrintWriter;
import java.util.concurrent.Callable;

import org.hibernate.SessionFactory;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.detector.db.HibernateSessionWrapper;
import com.linkedin.thirdeye.detector.email.EmailReportJobManager;

import io.dropwizard.servlets.tasks.Task;

public class EmailReportJobManagerTask extends Task {
  private final EmailReportJobManager manager;
  private final SessionFactory sessionFactory;

  public EmailReportJobManagerTask(EmailReportJobManager manager, SessionFactory sessionFactory) {
    super("email");
    this.manager = manager;
    this.sessionFactory = sessionFactory;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter)
      throws Exception {
    final String action = params.get("action").asList().get(0);
    new HibernateSessionWrapper<Void>(sessionFactory).execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        if ("start".equalsIgnoreCase(action)) {
          manager.start();
        } else if ("stop".equalsIgnoreCase(action)) {
          manager.stop();
        } else if ("reset".equalsIgnoreCase(action)) {
          manager.reset();
        }
        return null;
      }
    });
  }
}
