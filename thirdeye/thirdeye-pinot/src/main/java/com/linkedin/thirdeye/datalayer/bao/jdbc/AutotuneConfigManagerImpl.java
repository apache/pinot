package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.linkedin.thirdeye.datalayer.bao.AutotuneConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AutotuneConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.AutotuneConfigBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.ArrayList;
import java.util.List;


public class AutotuneConfigManagerImpl extends AbstractManagerImpl<AutotuneConfigDTO>
    implements AutotuneConfigManager {

  private final String FUNCTION_ID = "functionId";
  private final String AUTOTUNE_METHOD = "autoTuneMethod";
  private final String PERFORMANCE_EVALUATION_METHOD = "performanceEvaluationMethod";
  private final String START_TIME = "startTime";
  private final String END_TIME = "endTime";
  private final String GOAL = "goal";



  public AutotuneConfigManagerImpl() {
    super(AutotuneConfigDTO.class, AutotuneConfigBean.class);
  }

  private List<AutotuneConfigDTO> beansToDTOs(List<AutotuneConfigBean> list){
    List<AutotuneConfigDTO> result = new ArrayList<>();
    for(AutotuneConfigBean bean : list) {
      AutotuneConfigDTO dto = MODEL_MAPPER.map(bean, AutotuneConfigDTO.class);
      result.add(dto);
    }
    return result;
  }

  @Override
  public List<AutotuneConfigDTO> findAllByFunctionId(long functionId) {
    Predicate predicate = Predicate.EQ(FUNCTION_ID, functionId);
    List<AutotuneConfigBean> list = genericPojoDao.get(predicate, AutotuneConfigBean.class);
    return beansToDTOs(list);
  }


  @Override
  public List<AutotuneConfigDTO> findAllByFunctionIdAndAutotuneMethod(long functionId, String autoTuneMethod) {
    Predicate predicate = Predicate.AND( Predicate.EQ(FUNCTION_ID, functionId),
        Predicate.EQ(AUTOTUNE_METHOD, autoTuneMethod));
    List<AutotuneConfigBean> list = genericPojoDao.get(predicate, AutotuneConfigBean.class);
    return beansToDTOs(list);
  }

  @Override
  public List<AutotuneConfigDTO> findAllByFunctionIdAutotuneAndEvaluationMethod(long functionId,
      String autoTuneMethod, String performanceEvaluationMethod) {
    Predicate predicate = Predicate.AND(Predicate.EQ(FUNCTION_ID, functionId),
        Predicate.EQ(AUTOTUNE_METHOD, autoTuneMethod), Predicate.EQ(PERFORMANCE_EVALUATION_METHOD, performanceEvaluationMethod));

    List<AutotuneConfigBean> list = genericPojoDao.get(predicate, AutotuneConfigBean.class);
    return beansToDTOs(list);
  }

  @Override
  public List<AutotuneConfigDTO> findAllByFuctionIdAndWindow(long functionId, long startTime, long endTime) {
    Predicate predicate = Predicate.AND(Predicate.EQ(FUNCTION_ID, functionId),
        Predicate.GE(START_TIME, startTime), Predicate.LE(END_TIME, endTime));

    List<AutotuneConfigBean> list = genericPojoDao.get(predicate, AutotuneConfigBean.class);
    return beansToDTOs(list);
  }
}
