import Controller from '@ember/controller';
import { computed } from '@ember/object';

export default Controller.extend({
  stats: computed(
    'model.anomalyPerformance',
    function() {
    }
  )
});
