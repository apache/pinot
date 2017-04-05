package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;

public interface DashboardConfigManager extends AbstractManager<DashboardConfigDTO> {

  DashboardConfigDTO findByName(String name);
  List<DashboardConfigDTO> findByDataset(String dataset);
  List<DashboardConfigDTO> findActiveByDataset(String dataset);
  List<DashboardConfigDTO> findWhereNameLikeAndActive(String name);
}
