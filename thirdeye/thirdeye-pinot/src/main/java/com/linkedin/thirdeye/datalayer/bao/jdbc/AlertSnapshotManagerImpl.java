package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.linkedin.thirdeye.datalayer.bao.AlertSnapshotManager;
import com.linkedin.thirdeye.datalayer.dto.AlertSnapshotDTO;
import com.linkedin.thirdeye.datalayer.pojo.AlertSnapshotBean;


public class AlertSnapshotManagerImpl extends AbstractManagerImpl<AlertSnapshotDTO>
    implements AlertSnapshotManager {

  public AlertSnapshotManagerImpl() {
    super(AlertSnapshotDTO.class, AlertSnapshotBean.class);
  }
}
