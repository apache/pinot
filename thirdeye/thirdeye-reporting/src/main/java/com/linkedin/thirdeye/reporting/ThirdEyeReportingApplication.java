package com.linkedin.thirdeye.reporting;

import java.io.File;

import org.quartz.SchedulerException;
import com.linkedin.thirdeye.reporting.api.ReportScheduler;
import com.linkedin.thirdeye.reporting.resource.AdminResource;
import com.linkedin.thirdeye.reporting.resource.ReportsResource;
import com.linkedin.thirdeye.reporting.task.ReportingTask;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;


public class ThirdEyeReportingApplication extends Application<ThirdEyeReportingConfiguration> {

    public static void main(final String[] args) throws Exception {
        new ThirdEyeReportingApplication().run(args);
    }

    @Override
    public String getName() {
        return "ThirdEyeReporting";
    }

    @Override
    public void initialize(final Bootstrap<ThirdEyeReportingConfiguration> bootstrap) {
        bootstrap.addBundle(new ViewBundle());
        bootstrap.addBundle(new AssetsBundle("/assets/css", "/assets/css", null, "css"));
        bootstrap.addBundle(new AssetsBundle("/assets/js", "/assets/js", null, "js"));
        bootstrap.addBundle(new AssetsBundle("/assets/img", "/assets/img", null, "img"));
    }

    @Override
    public void run(final ThirdEyeReportingConfiguration configuration,
                    final Environment environment) throws SchedulerException {

     final ReportScheduler reportScheduler = new ReportScheduler(new File(configuration.getReportConfigPath()),
          configuration.getReportEmailTemplatePath(),
          configuration.getServerUri(),
          configuration.getDashboardUri());

      environment.lifecycle().manage(new Managed() {

        @Override
        public void start() throws Exception {
          reportScheduler.start();
        }

        @Override
        public void stop() throws Exception {
          reportScheduler.stop();

        }
      });

      // Resources
      environment.jersey().register(new ReportsResource(
          environment.metrics(), configuration.getReportConfigPath()));
      environment.jersey().register(new AdminResource());

      // Tasks
      environment.admin().addTask(new ReportingTask(reportScheduler));

    }

}
