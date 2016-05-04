package com.linkedin.thirdeye.dashboard;

public class HelperBundle extends HandlebarsHelperBundle<ThirdEyeDashboardConfig> {

  @Override
  protected void configureHandlebars(ThirdEyeDashboardConfig configuration) {
    //    DateHelper dateHelper = new DateHelper(config.getTimeZone());
    //    handlebars().registerHelper("date", dateHelper);
    //    handlebars().setPrettyPrint(true);
  }

}
