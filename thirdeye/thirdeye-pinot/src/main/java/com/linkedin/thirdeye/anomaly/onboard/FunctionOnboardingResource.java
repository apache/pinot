package com.linkedin.thirdeye.anomaly.onboard;

import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import javax.validation.constraints.NotNull;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;


@Path("/function-onboard")
@Produces(MediaType.APPLICATION_JSON)
public class FunctionOnboardingResource {
  private AnomalyFunctionManager anomalyFunctionDAO;
  public FunctionOnboardingResource(){
    anomalyFunctionDAO = DAORegistry.getInstance().getAnomalyFunctionDAO();
  }

  /**
   * Create a dummy function with given function name
   * Note that, this is a turn-around solution to resolve no credential information on worker side. This should be
   * fixed once the pipeline refactoring is done.
   * TODO remove this endpoint once pipeline refactoring is done.
   * @param functionName
   * @return a dummy function with given function name
   */
  @POST
  @Path("/create-function")
  public AnomalyFunctionDTO createAnomalyFunction(@NotNull @QueryParam("name") String functionName ) {
    AnomalyFunctionDTO functionDTO = new AnomalyFunctionDTO();
    functionDTO.setFunctionName(functionName);
    functionDTO.setMetricId(-1);
    functionDTO.setIsActive(false);
    anomalyFunctionDAO.save(functionDTO);
    return functionDTO;
  }
}

