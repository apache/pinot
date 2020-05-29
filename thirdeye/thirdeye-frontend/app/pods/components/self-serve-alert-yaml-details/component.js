/**
 * This component displays an alert summary section for users to see alert properties at a glance.
 * Initially used for consistency in both alert index page and single alert details page.
 * We use slightly different sub class names for positioning based on use case.
 * @module components/self-serve-alert-yaml-details
 * @property {Object} alertData    - alert properties
 * @property {Boolean} isLoadError - was there an error loading the data
 * @property {String} displayMode  - is the use case part of a list or standalone? 'list' || 'single'
 * @example
    {{#self-serve-alert-yaml-details
      alertData=alertData
      isLoadError=false
      displayMode="list"
      toggleActivation=(action "toggleActivation")
    }}
      ...additional case-specific header content
    {{/self-serve-alert-yaml-details}}
 * @exports self-serve-alert-yaml-details
 * @authors lohuynh and hjackson
 */

import Component from '@ember/component';
import { setProperties, get } from '@ember/object';

export default Component.extend({
  valueClassSuffix: '',
  modeSubClass: 'list',
  toggleActivation: null, // passed from parent - do not set

  init() {
    this._super(...arguments);
    const mode = get(this, 'displayMode');
    if (mode === 'single') {
      setProperties(this, {
        valueClassSuffix: '-solo',
        modeSubClass: 'solo'
      });
    }
  },

  actions: {
    /**
     * send action to parent
     */
    toggleAlertActivation() {
      this.get('toggleActivation')();
    }
  }
});
