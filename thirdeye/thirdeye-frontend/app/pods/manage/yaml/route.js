/**
 * Handles the 'alert details' route.
 * @module manage/alert/route
 * @exports manage alert model
 */
import Route from '@ember/routing/route';
import RSVP from 'rsvp';
import { set, get } from '@ember/object';
import { inject as service } from '@ember/service';

export default Route.extend({
  notifications: service('toast'),

  async model(params) {
    const id = params.alert_id;

    //detection alert fetch
    const url = `/detection/${id}`;
    const postProps = {
      method: 'get',
      headers: { 'content-type': 'application/json' }
    };
    const notifications = this.get('notifications');

    await fetch(url, postProps).then((res) => {
      res.json().then((result) => {
        if (result && result.yaml) {
          set(this, 'detectionYaml', result);
        }
      });

      if (res && res.active) {
        notifications.success('Save alert yaml successfully.', 'Saved');
      }
    }).catch((ex) => {
      notifications.error('Save alert yaml file failed.', 'Error');
    });

    //subscription group fetch
    const url2 = `/detection/subscription-groups/${id}`;//dropdown of subscription groups
    const postProps2 = {
      method: 'get',
      headers: { 'content-type': 'application/json' }
    };

    await fetch(url2, postProps2).then((res) => {
      res.json().then((result) => {
        if (result && result.yaml) {
          set(this, 'detectionSettingsYaml', result);
        }
      });

      if (res && res.active) {
        notifications.success('Save alert yaml successfully.', 'Saved');
      }
    }).catch((ex) => {
      notifications.error('Save alert yaml file failed.', 'Error');
    });

    return RSVP.hash({
      id,
      detectionYaml: get(this, 'detectionYaml'),
      detectionSettingsYaml: get(this, 'detectionSettingsYaml')
    });
  }
});
