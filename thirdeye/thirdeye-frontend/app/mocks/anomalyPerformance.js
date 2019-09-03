/**
 * Mock data for anomaly performance requests
 */
export const performanceData = (numAlerts) => {
  return {
    totalAlerts: numAlerts || 3,   // displayed as 'number of anomalies' on frontend
    totalResponses:'1',
    newTrend:'0',
    falseAlarm:'1',
    responseRate:'0.8571428571428571',
    trueAnomalies:'2',
    userReportAnomaly:'0',
    weightedPrecision:'0.46153846153846156',
    precision:'0.50',
    recall:'1.0'
  };
};

export default {
  performanceData
};
