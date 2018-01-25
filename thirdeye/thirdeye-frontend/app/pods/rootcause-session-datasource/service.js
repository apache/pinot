import Service from '@ember/service';
import fetch from 'fetch';
import { checkStatus } from 'thirdeye-frontend/utils/utils';

export default Service.extend({
  loadAsync(sessionId) {
    if (!sessionId) { return; }

    return fetch(`/session/${sessionId}`).then(checkStatus);
  },

  saveAsync(session) {
    const jsonString = JSON.stringify(session);

    return fetch(`/session/`, { method: 'POST', body: jsonString }).then(checkStatus);
  }
});
