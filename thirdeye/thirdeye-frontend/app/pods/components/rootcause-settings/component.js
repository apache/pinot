import Ember from 'ember';

export default Ember.Component.extend({
  context: null, // { urns, anomalyRange, baselineRange, analaysisRange }

  onChange: null, // function (context)

  urnString: null,

  anomalyRangeStart: null,

  anomalyRangeEnd: null,

  baselineRangeStart: null,

  baselineRangeEnd: null,

  analysisRangeStart: null,

  analysisRangeEnd: null,

  _updateFromContext() {
    const { urns, anomalyRange, baselineRange, analysisRange } = this.get('context');
    this.setProperties({ urnString: [...urns].join(','),
      anomalyRangeStart: anomalyRange[0], anomalyRangeEnd: anomalyRange[1],
      baselineRangeStart: baselineRange[0], baselineRangeEnd: baselineRange[1],
      analysisRangeStart: analysisRange[0], analysisRangeEnd: analysisRange[1]
    });
  },

  didUpdateAttrs() {
    this._super(...arguments);
    this._updateFromContext();
  },

  didInsertElement() {
    this._super(...arguments);
    this._updateFromContext();
  },

  actions: {
    updateContext() {
      const { urnString, anomalyRangeStart, anomalyRangeEnd, baselineRangeStart, baselineRangeEnd, analysisRangeStart, analysisRangeEnd } =
        this.getProperties('urnString', 'anomalyRangeStart', 'anomalyRangeEnd', 'baselineRangeStart', 'baselineRangeEnd', 'analysisRangeStart', 'analysisRangeEnd');
      const onChange = this.get('onChange');
      if (onChange != null) {
        const urns = new Set(urnString.split(','));
        const anomalyRange = [parseInt(anomalyRangeStart), parseInt(anomalyRangeEnd)];
        const baselineRange = [parseInt(baselineRangeStart), parseInt(baselineRangeEnd)];
        const analysisRange = [parseInt(analysisRangeStart), parseInt(analysisRangeEnd)];
        const newContext = { urns, anomalyRange, baselineRange, analysisRange };
        onChange(newContext);
      }
    }
  }

});
