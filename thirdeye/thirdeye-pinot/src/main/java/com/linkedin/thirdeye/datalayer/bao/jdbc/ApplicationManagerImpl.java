package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.linkedin.thirdeye.datalayer.bao.ApplicationManager;
import com.linkedin.thirdeye.datalayer.dto.ApplicationDTO;
import com.linkedin.thirdeye.datalayer.pojo.ApplicationBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.ArrayList;
import java.util.List;


public class ApplicationManagerImpl extends AbstractManagerImpl<ApplicationDTO>
    implements ApplicationManager {

  public ApplicationManagerImpl() {
    super(ApplicationDTO.class, ApplicationBean.class);
  }

  public List<ApplicationDTO> findByName(String name) {
    Predicate predicate = Predicate.EQ("application", name);
    List<ApplicationBean> list = genericPojoDao.get(predicate, ApplicationBean.class);
    List<ApplicationDTO> result = new ArrayList<>();
    for (ApplicationBean abstractBean : list) {
      ApplicationDTO dto = MODEL_MAPPER.map(abstractBean, ApplicationDTO.class);
      result.add(dto);
    }
    return result;
  }
}
