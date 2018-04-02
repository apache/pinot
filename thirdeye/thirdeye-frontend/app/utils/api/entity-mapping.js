/**
 * Endpoints for entity mapping modal
 */
export const entityMappingApi = {
  createUrl: `/entityMapping/create`,
  deleteUrl: `/entityMapping/delete`,
  getRelatedEntitiesUrl: `/entityMapping/view/fromURN`,
  getDatasetsUrl: `/data/datasets`,
  getServicesUrl: `/external/services/all`,
  metricAutoCompleteUrl(str) {
    return `/data/autocomplete/metric?name=${str}`;
  },
  getRelatedEntitiesDataUrl(urns) {
    return `/rootcause/raw?framework=identity&urns=${urns}`;
  }
};

export default entityMappingApi;
