package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.linkedin.thirdeye.datalayer.bao.FunctionAutoTuneConfigManager;
import com.linkedin.thirdeye.datalayer.dto.FunctionAutoTuneConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.FunctionAutoTuneConfigBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.ArrayList;
import java.util.List;

public class FunctionAutoTuneConfigManagerImpl extends AbstractManagerImpl<FunctionAutoTuneConfigDTO>
    implements FunctionAutoTuneConfigManager {

  private final String FUNCTION_ID = "functionId";
  private final String AUTOTUNE_METHOD = "autoTuneMethod";
  private final String PERFORMANCE_EVALUATION_METHOD = "performanceEvaluationMethod";
  private final String START_TIME = "startTime";
  private final String END_TIME = "endTime";


  public FunctionAutoTuneConfigManagerImpl() {
    super(FunctionAutoTuneConfigDTO.class, FunctionAutoTuneConfigBean.class);
  }

  private List<FunctionAutoTuneConfigDTO> beansToDTOs(List<FunctionAutoTuneConfigBean> list){
    List<FunctionAutoTuneConfigDTO> result = new ArrayList<>();
    for(FunctionAutoTuneConfigBean bean : list) {
      FunctionAutoTuneConfigDTO dto = MODEL_MAPPER.map(bean, FunctionAutoTuneConfigDTO.class);
      result.add(dto);
    }
    return result;
  }

  @Override
  public List<FunctionAutoTuneConfigDTO> findAllByFunctionId(long functionId) {
    Predicate predicate = Predicate.EQ(FUNCTION_ID, functionId);
    List<FunctionAutoTuneConfigBean> list = genericPojoDao.get(predicate, FunctionAutoTuneConfigBean.class);
    return beansToDTOs(list);
  }

  @Override
  public List<FunctionAutoTuneConfigDTO> findAllByFunctionIdAndAutoTuneMethod(long functionId, String autoTuneMethod) {
    Predicate predicate = Predicate.AND( Predicate.EQ(FUNCTION_ID, functionId),
        Predicate.EQ(AUTOTUNE_METHOD, autoTuneMethod));
    List<FunctionAutoTuneConfigBean> list = genericPojoDao.get(predicate, FunctionAutoTuneConfigBean.class);
    return beansToDTOs(list);
  }

  @Override
  public List<FunctionAutoTuneConfigDTO> findAllByFunctionIdAutoTuneAndEvaluationMethod(long functionId,
      String autoTuneMethod, String performanceEvaluationMethod) {
    Predicate predicate = Predicate.AND(Predicate.EQ(FUNCTION_ID, functionId),
        Predicate.EQ(AUTOTUNE_METHOD, autoTuneMethod), Predicate.EQ(PERFORMANCE_EVALUATION_METHOD, performanceEvaluationMethod));

    List<FunctionAutoTuneConfigBean> list = genericPojoDao.get(predicate, FunctionAutoTuneConfigBean.class);
    return beansToDTOs(list);
  }

  @Override
  public List<FunctionAutoTuneConfigDTO> findAllByFuctionIdAndWindow(long functionId, long startTime, long endTime) {
    Predicate predicate = Predicate.AND(Predicate.EQ(FUNCTION_ID, functionId),
        Predicate.GE(START_TIME, startTime), Predicate.LE(END_TIME, endTime));

    List<FunctionAutoTuneConfigBean> list = genericPojoDao.get(predicate, FunctionAutoTuneConfigBean.class);
    return beansToDTOs(list);
  }
}
