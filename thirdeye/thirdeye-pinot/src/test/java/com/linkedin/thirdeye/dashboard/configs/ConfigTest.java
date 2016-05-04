package com.linkedin.thirdeye.dashboard.configs;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.dashboard.Utils;

public class ConfigTest {

  public static void main(String[] args) throws Exception {
    String rootDirectory = "/tmp/config_root_dir";

    List<MetricExpression> metricExpressions= new ArrayList<>();
    metricExpressions.add(new MetricExpression("count", "__COUNT"));
    String filterClause =
        "%7B%22countryCode%22%3A%5B%22us%22%2C%22in%22%5D%2C%22environment%22%3A%5B%22prod-lva1%22%2C%22prod-ltx1%22%5D%7D";
    String collectionName = "thirdeyeAbook";

    DashboardConfig dashBoardConfig = new DashboardConfig();
    dashBoardConfig.setDashboardId(1);
    dashBoardConfig.setDashboardName("dashboard1");
    dashBoardConfig.setCollectionName(collectionName);
    dashBoardConfig.setMetricExpressions(metricExpressions);
    dashBoardConfig.setFilterClause(filterClause);

    DashboardConfig dashBoardConfig2 = new DashboardConfig();
    dashBoardConfig2.setDashboardId(2);
    dashBoardConfig2.setDashboardName("dashboard2");
    dashBoardConfig2.setCollectionName(collectionName);
    dashBoardConfig2.setMetricExpressions(metricExpressions);
    dashBoardConfig2.setFilterClause(filterClause);

    DashboardConfig dashBoardConfig3 = new DashboardConfig();
    dashBoardConfig3.setDashboardId(3);
    dashBoardConfig3.setDashboardName("dashboard3");
    dashBoardConfig3.setCollectionName(collectionName);
    dashBoardConfig3.setMetricExpressions(metricExpressions);
    dashBoardConfig3.setFilterClause(filterClause);

    FileBasedConfigDAOFactory configDAOFactory = new FileBasedConfigDAOFactory(rootDirectory);
    AbstractConfigDAO<DashboardConfig> configDAO = configDAOFactory.getDashboardConfigDAO();

    String id = "1";
    configDAO.create(collectionName, id, dashBoardConfig);

    String id2 = "2";
    configDAO.create(collectionName, id2, dashBoardConfig2);

    String id3 = "3";
    configDAO.create(collectionName, id3, dashBoardConfig3);

    DashboardConfig config = configDAO.findById(collectionName, id);
    System.out.println(config.toJSON());

    List<String> dashboards = Utils.getDashboards(configDAO, collectionName);
    System.out.println(dashboards);

    List<MetricExpression> metricExpressionsFromConfig =
        Utils.getMetricExpressions(configDAO, collectionName, "1");
    System.out.println(metricExpressionsFromConfig);
  }

}
