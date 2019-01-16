import Component from '@ember/component';
import { computed, set, get, getProperties } from '@ember/object';

export default Component.extend({

  showYAMLModal: computed(
    'showAnomalyModal',
    'showNotificationModal',
    function() {
      const {
        showAnomalyModal,
        showNotificationModal
      } = getProperties(this, 'showAnomalyModal', 'showNotificationModal');
      return showAnomalyModal || showNotificationModal;
    }
  ),

  headerText: computed(
    'YAMLField',
    function() {
      const YAMLField = get(this, 'YAMLField');
      return `YAML ${YAMLField} Documentation`;
    }
  ),

  actions: {
    /**
     * Handles the close event
     * @return {undefined}
     */
    onExit() {
      let YAMLField = get(this, 'YAMLField');
      set(this, `show${YAMLField}Modal`, false);
    }
  }
});
