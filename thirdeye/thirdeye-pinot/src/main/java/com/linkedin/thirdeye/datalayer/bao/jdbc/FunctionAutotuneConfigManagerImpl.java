package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.linkedin.thirdeye.datalayer.bao.FunctionAutotuneConfigManager;
import com.linkedin.thirdeye.datalayer.dto.FunctionAutotuneConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.FunctionAutotuneConfigBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.ArrayList;
import java.util.List;


public class FunctionAutotuneConfigManagerImpl extends AbstractManagerImpl<FunctionAutotuneConfigDTO>
    implements FunctionAutotuneConfigManager {

  private final String FUNCTION_ID = "functionId";
  private final String AUTOTUNE_METHOD = "autoTuneMethod";
  private final String PERFORMANCE_EVALUATION_METHOD = "performanceEvaluationMethod";
  private final String START_TIME = "startTime";
  private final String END_TIME = "endTime";
  private final String GOAL = "goal";



  public FunctionAutotuneConfigManagerImpl() {
    super(FunctionAutotuneConfigDTO.class, FunctionAutotuneConfigBean.class);
  }

  private List<FunctionAutotuneConfigDTO> beansToDTOs(List<FunctionAutotuneConfigBean> list){
    List<FunctionAutotuneConfigDTO> result = new ArrayList<>();
    for(FunctionAutotuneConfigBean bean : list) {
      FunctionAutotuneConfigDTO dto = MODEL_MAPPER.map(bean, FunctionAutotuneConfigDTO.class);
      result.add(dto);
    }
    return result;
  }

  @Override
  public List<FunctionAutotuneConfigDTO> findAllByFunctionId(long functionId) {
    Predicate predicate = Predicate.EQ(FUNCTION_ID, functionId);
    List<FunctionAutotuneConfigBean> list = genericPojoDao.get(predicate, FunctionAutotuneConfigBean.class);
    return beansToDTOs(list);
  }


  @Override
  public List<FunctionAutotuneConfigDTO> findAllByFunctionIdAndAutotuneMethod(long functionId, String autoTuneMethod) {
    Predicate predicate = Predicate.AND( Predicate.EQ(FUNCTION_ID, functionId),
        Predicate.EQ(AUTOTUNE_METHOD, autoTuneMethod));
    List<FunctionAutotuneConfigBean> list = genericPojoDao.get(predicate, FunctionAutotuneConfigBean.class);
    return beansToDTOs(list);
  }

  @Override
  public List<FunctionAutotuneConfigDTO> findAllByFunctionIdAutotuneAndEvaluationMethod(long functionId,
      String autoTuneMethod, String performanceEvaluationMethod) {
    Predicate predicate = Predicate.AND(Predicate.EQ(FUNCTION_ID, functionId),
        Predicate.EQ(AUTOTUNE_METHOD, autoTuneMethod), Predicate.EQ(PERFORMANCE_EVALUATION_METHOD, performanceEvaluationMethod));

    List<FunctionAutotuneConfigBean> list = genericPojoDao.get(predicate, FunctionAutotuneConfigBean.class);
    return beansToDTOs(list);
  }

  @Override
  public List<FunctionAutotuneConfigDTO> findAllByFuctionIdAndWindow(long functionId, long startTime, long endTime) {
    Predicate predicate = Predicate.AND(Predicate.EQ(FUNCTION_ID, functionId),
        Predicate.GE(START_TIME, startTime), Predicate.LE(END_TIME, endTime));

    List<FunctionAutotuneConfigBean> list = genericPojoDao.get(predicate, FunctionAutotuneConfigBean.class);
    return beansToDTOs(list);
  }

  @Override
  public List<FunctionAutotuneConfigDTO> findAllByFunctionIdWindowGoal(long functionId, long startTime, long endTime,
      double goal) {
    Predicate predicate = Predicate.AND(Predicate.EQ(FUNCTION_ID, functionId),
        Predicate.GE(START_TIME, startTime), Predicate.LE(END_TIME, endTime), Predicate.EQ(GOAL, goal));
    List<FunctionAutotuneConfigBean> list = genericPojoDao.get(predicate, FunctionAutotuneConfigBean.class);
    return beansToDTOs(list);
  }
}
