import { get } from '@ember/object';
import Component from '@ember/component';

export default Component.extend({
  showLegend: false, // boolean

  showToolbar: false, // boolean

  showSlider: false, // boolean
  
  entities: null, // {}

  selectedUrns: null, // Set

  invisibleUrns: null, // Set

  context: null, // {}

  timeseriesMode: null, // 'absolute', 'relative', 'log', 'split'

  chartSelectedUrns: null, // Set

  timeseries: null, // {}

  /**
   * urns of the entities to be focused on the chart
   * @type {Set}
   */
  focusedUrns: new Set(),

  onVisibility: null, // function (updates)

  onSelection: null, // function (data)

  onLegendHover: null, // function (urns)

  onPrimaryChange: null, // function (data)

  onContext: null, // func (context)

  onChart: null, // func (timeseriesMode)

  chartOnHover: null, // function (outputUrns, d)

  onComparisonChange: null, // function (start, end, compareMode)

  actions: {
    onVisibility(updates) {
      const onVisibility = get(this, 'onVisibility');
      if (onVisibility) {
        onVisibility(updates);
      }
    },

    onSelection(data) {
      const onSelection = get(this, 'onSelection');
      if (onSelection) {
        onSelection(data);
      }
    },

    onLegendHover(urns) {
      const onLegendHover = get(this, 'onLegendHover');
      if (onLegendHover) {
        onLegendHover(urns);
      }
    },

    onPrimaryChange(data) {
      const onPrimaryChange = get(this, 'onPrimaryChange');
      if (onPrimaryChange) {
        onPrimaryChange(data);
      }
    },

    onContext(context) {
      const onContext = get(this, 'onContext');
      if (onContext) {
        onContext(context);
      }
    },

    onChart(timeSeriesMode) {
      const onChart = get(this, 'onChart');
      if (onChart) {
        onChart(timeSeriesMode);
      }
    },

    chartOnHover(outputUrns, d) {
      const chartOnHover = get(this, 'chartOnHover');
      if (chartOnHover) {
        chartOnHover(outputUrns, d);
      }
    },

    onComparisonChange(start, end, compareMode) {
      const onComparisonChange = get(this, 'onComparisonChange');
      if (onComparisonChange) {
        onComparisonChange(start, end, compareMode);
      }
    }

  }

});
