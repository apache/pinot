/**
 * Endpoints for create event
 */
export const createEventApi = {
  createEventUrl(startTime, endTime, eventName) {
    return `/events/create?eventName=${eventName}&startTime=${startTime}&endTime=${endTime}`;
  }
};

export default createEventApi;
