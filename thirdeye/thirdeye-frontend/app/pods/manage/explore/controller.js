/**
 * Controller for Alert Landing and Details Page
 * @module manage/alert
 * @exports manage/alert
 */
import Controller from '@ember/controller';
import { inject as service } from '@ember/service';
import { putAlertActiveStatus } from 'thirdeye-frontend/utils/anomaly';
import { toastOptions } from 'thirdeye-frontend/utils/constants';

export default Controller.extend({

  notifications: service('toast'),

  actions: {

    /**
     * toggle the active status of alert being displayed
     * @method toggleActivation
     * @return {undefined}
     */
    toggleActivation() {
      const detectionConfigId = this.get('model.alertId');
      putAlertActiveStatus(detectionConfigId, !this.get('model.alertData.isActive'))
        .then(() => this.send('refreshModel'))
        .catch(error => {
          this.get('notifications')
            .error(`Failed to set active flag of detection config ${detectionConfigId}: ${(typeof error === 'object' ? error.message : error)}`,
              'Error',
              toastOptions);
        });
    }
  }
});
