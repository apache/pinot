package com.linkedin.thirdeye.datalayer;

public class AnomalyFeedbackDAOTest {


  public static void main(String[] args) throws Exception {
    // TODO: 8/31/16 use test/resources/schema/database.sql to create db schema

  /*  URL url = AnomalyFeedbackDAOTest.class.getResource("/persistence.yml");
    PersistenceApp.testWithPersistenceFile(new String[]{"db" ,"migrate", url.getFile()});
    File configFile = new File(url.toURI());
    DaoProviderUtil.init(configFile);
    AnomalyFeedbackDAO dao = DaoProviderUtil.getInstance(AnomalyFeedbackDAO.class);

    Connection conn = dao.getConnection();

    //INSERT 3 rows
    for (int i = 0; i < 3; i++) {
      AnomalyFeedback feedback = new AnomalyFeedback();
      feedback.setComment("asdsad-" + i);
      feedback.setStatus(FeedbackStatus.NEW);
      feedback.setFeedbackType(AnomalyFeedbackType.ANOMALY);
      Long feedbackId = dao.save(feedback);
      System.out.println("Saved Feedback ID:" + feedbackId);
    }
    //READ ALL ROWS
    ResultSet selectionResultSet =
        conn.createStatement().executeQuery("select * from anomaly_feedback");
    int count = 0;
    while (selectionResultSet.next()) {
      count++;
      System.out.println(selectionResultSet.getString(2));
    }
    System.out.println("Results found:" + count);
    //FIND BY ID
    AnomalyFeedback anomalyFeedback = dao.findById(1L);
    System.out.println("Retreived " + anomalyFeedback);

    //FIND BY PARAMS
    Map<String, Object> filters = new HashMap<>();
    filters.put("status", "NEW");
    List<AnomalyFeedback> results = dao.findByParams(filters);
    for (AnomalyFeedback result : results) {
      System.out.println("Retreived result: " + result);
    }

    //UPDATE TEST
    AnomalyFeedback updateFeedback = new AnomalyFeedback();
    updateFeedback.setId(1L);
    int updatedRows = dao.update(updateFeedback);
    System.out.println("Num rows Updated " + updatedRows);

    //READ THE UPDATED ROW
    AnomalyFeedback updatedFeedback = dao.findById(1L);
    System.out.println("Retreived updatedFeedback: " + updatedFeedback);

    //parameterized sql
    String parameterizedSQL = "select * from AnomalyFeedback where status = :status and feedbackType = :feedbackType";
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("status", FeedbackStatus.RESOLVED);
    parameterMap.put("feedbackType", AnomalyFeedbackType.NOT_ANOMALY);

    List<AnomalyFeedback> feedbacks = dao.executeParameterizedSQL(parameterizedSQL , parameterMap);
    System.out.println("result executing parameterized sql:"+ feedbacks);

    //DELETE TEST
    int numRowsDeleted = dao.deleteById(1L);
    System.out.println("Num rows Deleted " + numRowsDeleted);

    //READ THE DELETED ROW
    AnomalyFeedback deletedFeedback = dao.findById(1L);
    System.out.println("Retreived deletedFeedback must be null: " + deletedFeedback);*/
  }
}
