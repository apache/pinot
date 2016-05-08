package com.linkedin.thirdeye.dashboard;

public class HelperBundle extends HandlebarsHelperBundle<ThirdEyeDashboardConfiguration> {

  @Override
  protected void configureHandlebars(ThirdEyeDashboardConfiguration configuration) {
    //    DateHelper dateHelper = new DateHelper(config.getTimeZone());
    //    handlebars().registerHelper("date", dateHelper);
    //    handlebars().setPrettyPrint(true);
  }

}
