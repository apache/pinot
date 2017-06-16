import Ember from 'ember';

//Todo: move this into a constants.js file
const COLOR_MAPPING = {
  blue: '#33AADA',
  orange: '#EF7E37',
  teal: '#17AFB8',
  purple: '#9896F2',
  red: '#FF6C70',
  green: '#6BAF49',
  pink: '#FF61b6'
};

export default Ember.Component.extend({

  /**
   * Maps each metric to a color / class 
   */
  didReceiveAttrs() {
    this._super(...arguments);

    const colors = {};
    const primaryMetric = this.get('primaryMetric');
    const relatedMetric = this.get('relatedMetrics');
    const metrics = [primaryMetric, ...relatedMetric];
    metrics.forEach((metric) => {
      const name = metric.metricName;
      const color = metric.color || 'blue';
      colors[`${name}-current`] = COLOR_MAPPING[color];
      colors[`${name}-baseline`] = COLOR_MAPPING[color];
    })
    this.set('colors', colors);
  },

  tagName: 'div',
  classNames: ['anomaly-graph'],
  primaryMetric: {},
  relatedMetrics: [],
  showGraphLegend: true,
  colors: {},


  showLegend: false,

  /**
   * Graph Legend config
   */
  legend: Ember.computed('showLegend', function() {
    const showLegend = this.get('showLegend');
    return {
      position: 'inset',
      show: showLegend
    }
  }),

  /**
   * Graph Zoom config
   */
  zoom: {
    enabled: true,
    // Todo: add onZoom action handler
    // onzoom: function() {
    // }
  },

  /**
   * Graph Point Config
   */
  point: Ember.computed(
    'showGraphLegend', 
    function() {
      return {
        show: false,
      }
    }
  ),

  /**
   * Graph axis config
   */
  axis: Ember.computed(
    'primaryMetric', 
    function () {
      return {
        y: {
          show: true
        }, 
        x: {
          type: 'timeseries',
          show: true,
          tick: {
            fit: false
          },
          // TODO: add the extent functionality
          // extent: [...this.get('anomaly.dates')].slice(1,2)
        }
      }
    }
  ),

  /**
   * Graph Subchart Config
   */
  subchart: Ember.computed('showGraphLegend',
    function() {
      return {
        show: this.get('showGraphLegend')
      }
    }
  ),

  /**
   * Graph Height Config
   */
  size: Ember.computed('showGraphLegend',
    function() {
      const height = this.get('showGraphLegend') ? 400 : 200;
      return {
        height
      }
    }
  ),

  /**
   * Data massages primary Metric into a Column
   */
  primaryMetricColumn: Ember.computed(
    'primaryMetric',
    'primaryMetric.isSelected',
    function() {
      const primaryMetric = this.get('primaryMetric');

      const { baselineValues, currentValues } = primaryMetric.subDimensionContributionMap['All'];
      return [
        [`${primaryMetric.metricName}-current`, ...currentValues],
        [`${primaryMetric.metricName}-baseline`, ...baselineValues]
      ]
    }
  ),

  /**
   * Data massages relatedMetrics into Columns
   */
  relatedMetricsColumn: Ember.computed(
    'relatedMetrics',
    'relatedMetrics.@each.isSelected',
    function() {
      const columns = [];
      const relatedMetrics = this.get('relatedMetrics') || [];
      
      relatedMetrics
        .filterBy('isSelected')
        .forEach((metric)  => {
          if (!metric) { return }
          const { baselineValues, currentValues } = metric.subDimensionContributionMap['All'];
          columns.push([`${metric.metricName}-current`, ...currentValues])
          columns.push([`${metric.metricName}-baseline`, ...baselineValues])
        })
      return columns;
    }
  ),

  /**
   * Derives x axis from the primary metric
   */
  chartDates: Ember.computed(
    'primaryMetric.timeBucketsCurrent',
    function() {
      return ['date', ...this.get('primaryMetric.timeBucketsCurrent')];
    }
  ),

  /**
   * Aggregates data for chart
   */
  data: Ember.computed(
    'primaryMetricColumn',
    'relatedMetricsColumn',
    'chartDates',
    'colors',
    function() {
      return {
        columns: [
          this.get('chartDates'),
          ...this.get('primaryMetricColumn'),
          ...this.get('relatedMetricsColumn')
        ],
        type: 'line',
        x: 'date',
        xFormat: '%Y-%m-%d %H:%M',
        style: 'dashed',
        colors: this.get('colors')
      }
    }
  ),

  /**
   * Data massages Primary Metric's region
   * and assigns color class
   */
  primaryRegions: Ember.computed('primaryMetric', function() {
    const primaryMetric = this.get('primaryMetric');
    return primaryMetric.regions.map((region) => {
      return {
        axis: 'x',
        start: region.start,
        end: region.end,
        tick: {
          format: '%m %d %Y'
        },
        class: `c3-region--${primaryMetric.color}`
      }

    })
  }),

  /**
   * Data massages Primary Metric's region
   * and assigns color class
   */
  relatedRegions: Ember.computed(
    'relatedMetrics',
    'relatedMetrics.@each.isSelected', 
    function() {
      const relatedMetrics = this.get('relatedMetrics');
      const regions = [];
      relatedMetrics
      .filterBy('isSelected')
      .forEach((metric)=> {
        const metricRegions = metric.regions.map((region) => {
          return {
            axis: 'x',
            start: region.start,
            end: region.end,
            tick: {
              format: '%m %d %Y'
            },
            class: `c3-region--${metric.color}`
          }
        })
        regions.push(...metricRegions);
      })
      return regions;
    }
  ),


  /**
   * Aggregates chart regions 
   */
  regions: Ember.computed('primaryRegions', 'relatedRegions', function() {
    return [...this.get('primaryRegions'), ...this.get('relatedRegions')];
  }),

  actions: {
    onSelection() {
      this.attrs.onSelection(...arguments);
    },
    onToggle() {
      this.toggleProperty('showGraphLegend');
    },
  }
});
