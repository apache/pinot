/**
 * This component displays an alert summary section for users to see alert properties at a glance.
 * Initially used for consistency in both alert index page and single alert details page.
 * We use slightly different sub class names for positioning based on use case.
 * @module components/self-serve-alert-details
 * @property {Object} alertData    - alert properties
 * @property {Boolean} isLoadError - was there an error loading the data
 * @property {String} displayMode  - is the use case part of a list or standalone? 'list' || 'single'
 * @example
    {{#self-serve-alert-details
      alertData=alertData
      isLoadError=false
      displayMode="list"
    }}
      ...additional case-specific header content
    {{/self-serve-alert-details}}
 * @exports self-serve-alert-details
 * @author smcclung
 */

import Component from '@ember/component';
import { setProperties, get } from '@ember/object';

export default Component.extend({
  valueClassSuffix: '',
  modeSubClass: 'list',

  init() {
    this._super(...arguments);
    const mode = get(this, 'displayMode');
    if (mode === 'single') {
      setProperties(this, {
        valueClassSuffix: '-solo',
        modeSubClass: 'solo'
      });
    }
  }
});
