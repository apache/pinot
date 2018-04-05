/**
 * Endpoints for create event
 */
export const createEventApi = {
  createEventUrl(startTime, endTime, eventName, countryCode) {
    return `/events/create?eventName=${eventName}&startTime=${startTime}&endTime=${endTime}&countryCode=${countryCode}`;
  }
};

export default createEventApi;
