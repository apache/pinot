import Component from "@ember/component";
import { computed } from '@ember/object';

export default Component.extend({
  isModifiedAvailable: computed(
    'record.updateTime',
    function() {
      const record = this.get('record');
      return record.get('updateTime') ? true : false;
    }
  )
});
