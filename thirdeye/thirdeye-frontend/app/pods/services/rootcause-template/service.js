import Service from '@ember/service';
import fetch from 'fetch';
import { checkStatus } from 'thirdeye-frontend/utils/utils';

export default Service.extend({
  findByMetricAsync(metricId) {
    if (!metricId) { return; }
    return fetch(`/rootcause/template/search?metricId=${metricId}`).then(checkStatus);
  },

  saveDimensionAnalysisAsync(metricUrn, includedDim, exlcudedDim, manualOrder, oneSideError, summarySize, dimDepth) {
    const included = includedDim.join(',');
    const excluded = exlcudedDim.join(',')
    return fetch(`/rootcause/template/saveDimensionAnalysis?metricUrn=${metricUrn}&includedDimension=${included}&exlcudedDimension=${excluded}&manualOrder=${manualOrder}&oneSideError=${oneSideError}&summarySize=${summarySize}&dimensionDepth=${dimDepth}`, 
        { method: 'POST' }).then(checkStatus);
  }
  
});
