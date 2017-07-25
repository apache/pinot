import Ember from 'ember';
import moment from 'moment';
import d3 from 'd3';

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
  init() {
    this._super(...arguments);
  },

  /**
   * Maps each metric and event to a color / class
   */
  didReceiveAttrs() {
    this._super(...arguments);

    const colors = {};
    const primaryMetric = this.get('primaryMetric');
    const relatedMetric = this.get('relatedMetrics') || [];
    const selectedMetrics = this.get('selectedMetrics') || [];
    const selectedDimensions = this.get('selectedDimensions') || [];
    const events = this.get('holidayEvents') || [];

    const data = [
      primaryMetric,
      ...relatedMetric,
      ...selectedMetrics,
      ...selectedDimensions];

    data.forEach((datum) => {
      const name = datum.metricName || datum.name;
      const { color = 'blue' } = datum;
      colors[`${name}-current`] = COLOR_MAPPING[color];
      colors[`${name}-expected`] = COLOR_MAPPING[color];
    });

    events.forEach((event) => {
      const { color = 'blue'} = event;
      colors[event.label] = COLOR_MAPPING[color];
    });

    this.set('colors', colors);
  },

  tagName: 'div',
  classNames: ['anomaly-graph'],
  primaryMetric: {},
  relatedMetrics: [],
  selectedMetrics: [],
  dimensions: [],
  selectedDimensions: [],

  showGraphLegend: true,
  colors: {},
  showSubChart: false,
  subchartStart: null,
  subchartEnd: null,
  showLegend: false,

  showTitle: false,
  height: 0,
  componentId: 'main-graph',

  initStart: null,
  initEnd: null,

  showEvents: false,
  showDimensions: false,
  showMetrics: false,
  events: [],

  enableZoom: false,

  // dd for primary metric
  primaryMetricId: Ember.computed('componentId', function() {
    return this.get('componentId') + '-primary-metric-';
  }),

  // id for related metrics
  relatedMetricId: Ember.computed('componentId', function() {
    return this.get('componentId') + '-related-metric-';
  }),

  // id for dimension
  dimensionId:  Ember.computed('componentId', function() {
    return this.get('componentId') + '-dimension-';
  }),

  // filtered events for graph
  holidayEvents: Ember.computed('events', function() {
    const events = this.get('events');
    const hiddenEvents = ['informed'];
    return events.filter((event) => {
      return !hiddenEvents.includes(event.eventType);
    });
  }),

  holidayEventsColumn: Ember.computed(
    'holidayEvents',
    function() {
      const events = this.get('holidayEvents');

      return events.map((event) => {
        const {
          // start,
          // end,
          score,
          label
        } = event;

        // const scores = (!end || start === end)
        //   ? [score, score]
        //   : [score];
        return [label, score];
      });
    }
  ),

  holidayEventsDatesColumn: Ember.computed(
    'holidayEvents',
    function() {
      const holidays = this.get('holidayEvents');

      return holidays.map((holiday) => {
        const { start, end } = holiday;

        const date = !end ? [end] : [start];

        return [`${holiday.label}-date`, date];
      });
    }
  ),

  /**
   * Graph Legend config
   */
  legend: Ember.computed('showGraphLegend', function() {
    const showGraphLegend = this.get('showGraphLegend');
    return {
      position: 'inset',
      show: showGraphLegend
    };
  }),

  /**
   * Graph Zoom config
   */
  zoom: Ember.computed(
    'onSubchartChange',
    'enableZoom',
    function() {
      const onSubchartBrush = this.get('onSubchartChange');
      return {
        enabled: this.get('enableZoom'),
        onzoomend: onSubchartBrush
      };
    }
  ),

  /**
   * Graph Point Config
   */
  point: Ember.computed(
    'showGraphLegend',
    function() {
      return {
        show: true,
        r: function(data) {
          const { id } = data;
          if (id.includes('current') || id.includes('expected')) {
            return 0;
          }

          return 5;
        }
      };
    }
  ),

  /**
   * Graph axis config
   */
  axis: Ember.computed(
    'primaryMetric.timeBucketsCurrent',
    'primaryMetric',
    'subchartStart',
    'subchartEnd',
    'showEvents',
    'minDate',
    'maxDate',
    function() {
      const dates = this.get('primaryMetric.timeBucketsCurrent');
      const subchartStart = this.get('subchartStart');
      const subchartEnd = this.get('subchartEnd');

      const min = dates.get('firstObject');
      const max = dates.get('lastObject');

      const startIndex = Math.floor(dates.length / 4);
      const endIndex = Math.ceil(dates.length * (3/4));
      const extentStart = subchartStart
        ? Number(subchartStart)
        : dates[startIndex];

      const extentEnd = subchartEnd
        ? Number(subchartEnd)
        : dates[endIndex];

      return {
        y: {
          show: true,
          min: 0,
          padding: {
            bottom: 0
          },
          tick: {
            format: d3.format('2s')
          }
        },
        y2: {
          show: false,
          label: 'events score',
          min: 0,
          max: 1,
          padding: {
            bottom: 0
          },
          tick: {
            values: [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
          }
        },
        x: {
          type: 'timeseries',
          show: true,
          min: this.get('minDate') || min,
          max: this.get('maxDate') || max,
          tick: {
            fit: false
            // format: function (x) { return new Date(x).toString(); }
          },
          extent: [extentStart, extentEnd]
        }
      };
    }
  ),

  /**
   * Graph Subchart Config
   */
  subchart: Ember.computed(
    'showLegend',
    'showSubchart',
    'showGraphLegend',
    function() {
      const showSubchart = this.get('showGraphLegend') || this.get('showSubchart');
      const onSubchartBrush = this.get('onSubchartChange');
      return {
        show: showSubchart,
        onbrush: onSubchartBrush
      };
    }
  ),

  /**
   * Graph Height Config
   */
  size: Ember.computed(
    'showLegend',
    'height',
    function() {
      const height = this.get('height')
        || this.get('showLegend') ? 400 : 200;
      return {
        height
      };
    }
  ),

  /**
   * Data massages primary Metric into a Column
   */
  primaryMetricColumn: Ember.computed(
    'primaryMetric',
    function() {
      const primaryMetric = this.get('primaryMetric');

      const { baselineValues, currentValues } = primaryMetric.subDimensionContributionMap['All'];
      return [
        [`${primaryMetric.metricName}-current`, ...currentValues],
        [`${primaryMetric.metricName}-expected`, ...baselineValues]
      ];
    }
  ),

  /**
   * Data massages relatedMetrics into Columns
   */
  selectedMetricsColumn: Ember.computed(
    'selectedMetrics',
    'selectedMetrics.@each',
    function() {
      const columns = [];
      const selectedMetrics = this.get('selectedMetrics') || [];

      selectedMetrics.forEach((metric)  => {
        if (!metric) { return; }
        const { baselineValues, currentValues } = metric.subDimensionContributionMap['All'];
        columns.push([`${metric.metricName}-current`, ...currentValues]);
        columns.push([`${metric.metricName}-expected`, ...baselineValues]);
      });
      return columns;
    }
  ),

  /**
   * Data massages dimensions into Columns
   */
  selectedDimensionsColumn: Ember.computed(
    'selectedDimensions',
    function() {
      const columns = [];
      const selectedDimensions = this.get('selectedDimensions') || [];

      selectedDimensions.forEach((dimension) => {
        const { baselineValues, currentValues } = dimension;
        columns.push([`${dimension.name}-current`, ...currentValues]);
        columns.push([`${dimension.name}-expected`, ...baselineValues]);
      });
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
    'selectedMetricsColumn',
    'selectedDimensionsColumn',
    'holidayEventsColumn',
    'holidayEventsDatesColumn',
    'chartDates',
    'colors',
    'onEventClick',
    function() {
      const {
        primaryMetricColumn,
        selectedMetricsColumn,
        selectedDimensionsColumn,
        holidayEventsColumn,
        holidayEventsDatesColumn
      } = this.getProperties(
        'primaryMetricColumn',
        'selectedMetricsColumn',
        'selectedDimensionsColumn',
        'holidayEventsDatesColumn',
        'holidayEventsColumn');

      const columns = [
        ...primaryMetricColumn,
        ...selectedMetricsColumn,
        ...selectedDimensionsColumn
      ];

      const holidayAxis = holidayEventsColumn
        .map(column => column[0])
        .reduce((hash, columnName) => {
          hash[columnName] = `${columnName}-date`;

          return hash;
        }, {});

      const holidayAxes = holidayEventsColumn
        .map(column => column[0])
        .reduce((hash, columnName) => {
          hash[columnName] = 'y2';

          return hash;
        }, {});

      const xAxis = columns
        .map(column => column[0])
        .reduce((hash, columnName) => {
          hash[columnName] = 'date';

          return hash;
        }, {});

      return {
        xs: Object.assign({}, xAxis, holidayAxis),
        axes: Object.assign({ y2: 'y2'}, holidayAxes),
        columns: [
          this.get('chartDates'),
          ...holidayEventsColumn,
          ...holidayEventsDatesColumn,
          ...columns
        ],
        type: 'line',
        // x: 'date',
        xFormat: '%Y-%m-%d %H:%M',
        colors: this.get('colors'),
        onclick: this.get('onEventClick')
      };
    }
  ),

  /**
   * Data massages Primary Metric's region
   * and assigns color class
   */
  primaryRegions: Ember.computed('primaryMetric', function() {
    const primaryMetric = this.get('primaryMetric');
    const { regions } = primaryMetric;

    if (!regions) { return []; }

    return regions.map((region) => {
      return {
        axis: 'x',
        start: region.start,
        end: region.end,
        tick: {
          format: '%m %d %Y'
        },
        class: `c3-region--${primaryMetric.color}`
      };

    });
  }),

  /**
   * Data massages Primary Metric's region
   * and assigns color class
   */
  relatedRegions: Ember.computed(
    'selectedMetrics',
    'selectedMetrics.@each',
    function() {
      const selectedMetrics = this.get('selectedMetrics') || [];
      const regions = [];
      selectedMetrics.forEach((metric)=> {

        if (!metric.regions) { return; }

        const metricRegions = metric.regions.map((region) => {
          return {
            axis: 'x',
            start: region.start,
            end: region.end,
            tick: {
              format: '%m %d %Y'
            },
            class: `c3-region--${metric.color}`
          };
        });
        regions.push(...metricRegions);
      });

      return regions;
    }
  ),

  /**
   * Config for tooltip
   */
  tooltip: {
    format: {
      title: function(d) {
        return moment(d).format('MM/DD hh:mm a');
      },
      value: function(val) {
        return d3.format('2s')(val);
      }
    }
  },


  /**
   * Aggregates chart regions
   */
  regions: Ember.computed('primaryRegions', 'relatedRegions', function() {
    return [...this.get('primaryRegions'), ...this.get('relatedRegions')];
  }),

  actions: {
    /**
     * Handles graph item selection
     */
    onSelection() {
      this.attrs.onSelection(...arguments);
    },
    onToggle() {
      this.toggleProperty('showGraphLegend');
    },

    /**
     * Scrolls to the appropriate tab on click
     */
    scrollToSection() {
      Ember.run.later(() => {
        Ember.$('#root-cause-analysis').get(0).scrollIntoView();
      });
    }
  }
});
