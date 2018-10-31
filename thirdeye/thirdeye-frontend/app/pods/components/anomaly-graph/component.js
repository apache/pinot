import $ from 'jquery';
import { set, computed } from '@ember/object';
import { later } from '@ember/runloop';
import Component from '@ember/component';
import moment from 'moment';
import d3 from 'd3';
import { eventWeightMapping } from 'thirdeye-frontend/actions/constants';

const COLOR_MAPPING = {
  blue: '#33AADA',
  orange: '#EF7E37',
  teal: '#17AFB8',
  purple: '#9896F2',
  red: '#FF6C70',
  green: '#6BAF49',
  pink: '#FF61b6',
  light_blue: '#65C3E8',
  light_orange: '#F6A16C',
  light_teal: '#68C5CD',
  light_purple: '#B2B0FA',
  light_red: '#FF999A',
  light_green: '#91C475',
  light_pink: '#FF91CF'
};

export default Component.extend({
  init() {
    this._super(...arguments);

    this.setProperties({
      _subchartStart: Number(this.get('subchartStart')),
      _subchartEnd: Number(this.get('subchartEnd'))
    });
  },

  // Helper function that builds the subchart region buttons
  buildSliderButton() {
    const componentId = this.get('componentId');
    const resizeButtons = d3.select(`#${componentId}.c3-chart-component`).selectAll('.resize');

    resizeButtons.append('circle')
      .attr('cx', 0)
      .attr('cy', 30)
      .attr('r', 10)
      .attr('fill', '#0091CA');
    resizeButtons.append('line')
      .attr('class', 'anomaly-graph__slider-line')
      .attr("x1", 0)
      .attr("y1", 27)
      .attr("x2", 0)
      .attr("y2", 33);

    resizeButtons.append('line')
      .attr('class', 'anomaly-graph__slider-line')
      .attr("x1", -5)
      .attr("y1", 27)
      .attr("x2", -5)
      .attr("y2", 33);

    resizeButtons.append('line')
      .attr('class', 'anomaly-graph__slider-line')
      .attr("x1", 5)
      .attr("y1", 27)
      .attr("x2", 5)
      .attr("y2", 33);
  },

  buildAnomalyRegionSlider(start, end) {
    const {
      componentId,
      regionStart,
      regionEnd,
      _subchartStart: subchartStart,
      _subchartEnd: subchartEnd
    } = this.getProperties(
      'componentId',
      'regionStart',
      'regionEnd',
      '_subchartStart',
      '_subchartEnd');

    start = start || subchartStart;
    end = end || subchartEnd;

    d3.select(`#${componentId} .anomaly-graph__region-slider`).remove();
    if (componentId !== 'main-graph') {return;}

    const focus = d3.select(`#${componentId}.c3-chart-component .c3-chart`);
    const { height, width } = d3.select(`#${componentId} .c3-chart .c3-event-rects`).node().getBoundingClientRect();
    const dates = this.get('primaryMetric.timeBucketsCurrent');
    const min = start ? moment(start).valueOf() : d3.min(dates);
    const max = end ? moment(end).valueOf() : d3.max(dates);

    const x = d3.time.scale()
      .domain([min, max])
      .range([0, width]);

    const brush = d3.svg.brush()
      .on("brushend", brushed.bind(this))
      .x(x)
      .extent([+regionStart, +regionEnd]);

    function brushed() {
      const e = brush.extent();
      const [ start, end ] = e;

      const regionStart = moment(start).valueOf();
      const regionEnd = moment(end).valueOf();
      const subchartStart = this.get('_subchartStart');
      const subchartEnd = this.get('_subchartEnd');

      this.setProperties({
        regionStart,
        regionEnd,
        subchartStart,
        subchartEnd
      });
    }

    focus.append('g')
      .attr('class', 'anomaly-graph__region-slider x brush')
      .call(brush)
      .selectAll('rect')
      .attr('y', 0)
      .attr('height', height);

    const resizeButton = focus.selectAll('.resize');
    const sliderHeight = height/2;
    resizeButton.append('circle')
      .attr('cx', 0)
      .attr('cy', sliderHeight)
      .attr('r', 10)
      .attr('fill', '#E55800');
    resizeButton.append('line')
      .attr('class', 'anomaly-graph__slider-line')
      .attr("x1", 0)
      .attr("y1", sliderHeight - 3)
      .attr("x2", 0)
      .attr("y2", sliderHeight + 3);

    resizeButton.append('line')
      .attr('class', 'anomaly-graph__slider-line')
      .attr("x1", -5)
      .attr("y1", sliderHeight - 3)
      .attr("x2", -5)
      .attr("y2", sliderHeight + 3);

    resizeButton.append('line')
      .attr('class', 'anomaly-graph__slider-line')
      .attr("x1", 5)
      .attr("y1", sliderHeight - 3)
      .attr("x2", 5)
      .attr("y2", sliderHeight + 3);
  },

  // Builds the Current/Expected legend for the graph
  buildCustomLegend() {
    const componentId = this.get('componentId');
    const chart = d3.select(`#${componentId}.c3-chart-component`);
    const legendText = this.get('legendText');

    const {
      dotted = { text: 'expected', color: 'blue'},
      solid = { text: 'current', color: 'blue' }
    }  = legendText;

    chart.insert('div', '.chart').attr('class', 'anomaly-graph__legend').selectAll('span')
      .data([dotted, solid])
      .enter().append('svg')
      .attr('class', 'anomaly-graph__legend-item')
      .attr('width', 80)
      .attr('height', 20)
      .attr('data-id', function (el) { return el.text; })
      .each(function (el) {
        const element = d3.select(this);

        element.append('text')
          .attr('x', 35)
          .attr('y', 12)
          .text(el.text);

        element.append('line')
          .attr('class', function(el) {
            return `anomaly-graph__legend-line anomaly-graph__legend-line--${el.color}`;
          })
          .attr('x1', 0)
          .attr('y1', 10)
          .attr('x2', 30)
          .attr('y2', 10)
          .attr('stroke-dasharray', (d) => {
            const dasharrayNum = (d === dotted) ? '10%' : 'none';
            return dasharrayNum;
          });
      });
    // Necessary so that it is 'thenable'
    return Promise.resolve();
  },

  willDestroyElement() {
    this._super(...arguments);

    const subchartStart = this.get('_subchartStart');
    const subchartEnd = this.get('_subchartEnd');


    this.setProperties({
      subchartStart,
      subchartEnd
    });
  },

  didRender(){
    this._super(...arguments);

    later(() => {
      this.buildSliderButton();
      // hiding this feature until fully fleshed out
      // this.buildAnomalyRegionSlider();
      this.buildCustomLegend().then(() => {
        this.notifyPhantomJS();
      });
    });
  },

  /**
   * Checks if the page is being viewed from phantomJS
   * and notifies it that the page is rendered and ready
   * for a screenshot
   */
  notifyPhantomJS() {
    if (typeof window.callPhantom === 'function') {
      window.callPhantom({message: 'ready'});
    }
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

    // This check is necessary so that it is only set once
    if (primaryMetric.isSelected === undefined) {
      set(primaryMetric, 'isSelected', true);
    }

    const data = [
      primaryMetric,
      ...relatedMetric,
      ...selectedMetrics,
      ...selectedDimensions];

    data.forEach((datum) => {
      const name = datum.metricName || datum.name;
      const { color } = datum;
      colors[`${name}-current`] = COLOR_MAPPING[color || 'blue'];
      colors[`${name}-expected`] = COLOR_MAPPING[color || 'orange'];
    });

    events.forEach((event) => {
      const { displayColor = 'blue'} = event;
      colors[event.displayLabel] = COLOR_MAPPING[displayColor];
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

  _subchartStart: 0,
  _subchartEnd: 0,

  analysisStart: 0,
  analysisEnd: 0,

  showLegend: false,
  // contains copy for the legend
  // currently supports 'dotted' and 'solid'
  legendText: {},

  showTitle: false,
  height: 0,
  componentId: 'main-graph',

  initStart: null,
  initEnd: null,
  anomalyRegionStart: 0,
  anomalyRegionEnd: 0,
  regionStart: 0,
  regionEnd: 0,

  showEvents: false,
  showDimensions: false,
  showMetrics: false,
  events: [],

  enableZoom: false,

  // padding for the anomaly graph (optional)
  padding: null,

  // dd for primary metric
  primaryMetricId: computed('componentId', function() {
    return this.get('componentId') + '-primary-metric-';
  }),

  // id for related metrics
  relatedMetricId: computed('componentId', function() {
    return this.get('componentId') + '-related-metric-';
  }),

  // id for dimension
  dimensionId:  computed('componentId', function() {
    return this.get('componentId') + '-dimension-';
  }),

  // filtered events for graph
  holidayEvents: computed('events', function() {
    return this.get('events');
    // const events = this.get('events');
    // const hiddenEvents = [];
    // return events.filter((event) => {
    //   return !hiddenEvents.includes(event.eventType);
    // });
  }),

  holidayEventsColumn: computed(
    'holidayEvents',
    function() {
      const events = this.get('holidayEvents');

      return events.map((event) => {
        const {
          // start,
          // end,
          displayScore,
          displayLabel
        } = event;

        // const scores = (!end || start === end)
        //   ? [score, score]
        //   : [score];
        return [displayLabel, displayScore];
      });
    }
  ),

  holidayEventsDatesColumn: computed(
    'holidayEvents',
    function() {
      const holidays = this.get('holidayEvents');

      return holidays.map((holiday) => {
        const { displayStart, displayEnd } = holiday;

        const start = displayStart;
        const end = displayEnd;

        const date = !end ? [end] : [start];

        return [`${holiday.displayLabel}-date`, date];
      });
    }
  ),

  /**
   * Graph Legend config
   */
  legend: computed('showGraphLegend', function() {
    const showGraphLegend = this.get('showGraphLegend');
    return {
      position: 'inset',
      show: showGraphLegend
    };
  }),

  /**
   * Graph Zoom config
   */
  zoom: computed(
    'onSubchartChange',
    'enableZoom',
    function() {
      const onSubchartBrush = this.get('onSubchartChange');
      return {
        enabled: this.get('enableZoom'),
        onzoomend: onSubchartBrush,
        rescale: true
      };
    }
  ),

  /**
   * Graph Point Config
   */
  point: computed(
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
  axis: computed(
    'chartDates',
    'primaryMetric',
    'subchartStart',
    'subchartEnd',
    'showEvents',
    'minDate',
    'maxDate',
    function() {
      const [ _, ...dates ] = this.get('chartDates');
      const subchartStart = this.get('_subchartStart')
        || this.get('subchartStart');
      const subchartEnd = this.get('_subchartEnd')
        || this.get('subchartEnd');
      const min = dates.get('firstObject');
      const max = dates.get('lastObject');
      const extentStart = Math.max(min, moment(subchartStart).valueOf());
      const extentEnd = Math.min(max, moment(subchartEnd).valueOf());

      const extentRange = (subchartStart && subchartEnd)
        ? [extentStart, extentEnd]
        : null;

      return {
        y: {
          show: true,
          // min: 0,
          tick: {
            format: d3.format('.2s')
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
          padding: 0,
          min: this.get('minDate') || min,
          max: this.get('maxDate') || max,
          tick: {
            fit: false
            // format: function (x) { return new Date(x).toString(); }
          },
          extent: extentRange
        }
      };
    }
  ),

  /**
   * Graph Subchart Config
   */
  subchart: computed(
    'showLegend',
    'showSubchart',
    'showGraphLegend',
    function() {
      const showSubchart = this.get('showGraphLegend') || this.get('showSubchart');
      return {
        show: showSubchart,
        onbrush: this.get('onbrush').bind(this)
      };
    }
  ),

  /**
   * Callback that handles the anomaly brush event
   */
  onbrush: function(dates) {
    const [ start, end ] = dates;
    const onSubchartBrush = this.get('onSubchartChange');
    const [ , ...graphDates ] = this.get('chartDates');
    const min = graphDates.get('firstObject');
    const max = graphDates.get('lastObject');

    if ((moment(start).valueOf() == min) && (moment(end).valueOf() == max)) {
      this.setProperties({
        _subchartStart: 0,
        _subchartEnd: 0
      });
    } else {
      this.setProperties({
        _subchartStart: moment(start).valueOf(),
        _subchartEnd: moment(end).valueOf()
      });

      onSubchartBrush && onSubchartBrush(dates);
    }
    // hiding this feature until fully fleshed out
    // this.buildAnomalyRegionSlider(start, end);

  },

  /**
   * Graph Height Config
   */
  size: computed(
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
  primaryMetricColumn: computed(
    'primaryMetric',
    'primaryMetric.isSelected',
    function() {
      const primaryMetric = this.get('primaryMetric');

      // Return data only when it's selected
      if (primaryMetric.isSelected) {
        const { baselineValues, currentValues } = primaryMetric.subDimensionContributionMap['All'];
        return [
          [`${primaryMetric.metricName}-current`, ...currentValues],
          [`${primaryMetric.metricName}-expected`, ...baselineValues]
        ];
      }
      return [
        [`${primaryMetric.metricName}-current`],
        [`${primaryMetric.metricName}-expected`]
      ];
    }
  ),

  /**
   * Data massages relatedMetrics into Columns
   */
  selectedMetricsColumn: computed(
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
  selectedDimensionsColumn: computed(
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
  chartDates: computed(
    'primaryMetric.timeBucketsCurrent',
    function() {
      return ['date', ...this.get('primaryMetric.timeBucketsCurrent')];
    }
  ),

  /**
   * Aggregates data for chart
   */
  data: computed(
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
        primaryMetricColumn = [],
        selectedMetricsColumn = [],
        selectedDimensionsColumn = [],
        holidayEventsColumn = [],
        holidayEventsDatesColumn = []
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
          ...columns,
          ...holidayEventsColumn,
          ...holidayEventsDatesColumn
        ],
        type: 'line',
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
  primaryRegions: computed('primaryMetric', function() {
    const primaryMetric = this.get('primaryMetric') || {};
    const {
      regions,
      color = 'orange'
    } = primaryMetric;

    if (!regions) { return []; }

    return regions.map((region) => {
      return {
        axis: 'x',
        start: region.start,
        end: region.end,
        tick: {
          format: '%m %d %Y'
        },
        class: `c3-region--${color}`
      };

    });
  }),

  /**
   * Data massages the main anomaly region
   */
  anomalyRegion: computed(
    'analysisStart',
    'analysisEnd',
    function() {
      const start = this.get('analysisStart');
      const end = this.get('analysisEnd');

      if (!(start && end)) { return []; }

      const region = {
        axis: 'x',
        start: Number(start),
        end: Number(end),
        tick: {
          format: '%m %d %Y'
        },
        class: `c3-region--dark-orange`
      };

      return [region];
    }
  ),

  /**
   * Data massages Primary Metric's region
   * and assigns color class
   */
  relatedRegions: computed(
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
      value: function(val, ratio, id) {
        const isMetric = ['current', 'expected'].some((category) => {
          return id.includes(category);
        });

        if (isMetric) {
          return d3.format('.3s')(val);
        } else {
          // do not return values if data point is an event

          const eventType = Object.keys(eventWeightMapping).find((key) => {
            if (val == eventWeightMapping[key]) {
              return key;
            }
          });

          return eventType || '';
        }
      }
    }
  },


  /**
   * Aggregates chart regions
   */
  regions: computed(
    'primaryRegions',
    'relatedRegions',
    'anomalyRegion',
    function() {
      return [
        ...this.get('primaryRegions'),
        ...this.get('relatedRegions'),
        ...this.get('anomalyRegion')
      ];
    }
  ),

  actions: {
    /**
     * Shows/Hides the primary metric
     */
    onPrimaryMetricToggle() {
      if (!this.attrs.onPrimaryClick) {
        this.toggleProperty('primaryMetric.isSelected');
      } else {
        this.attrs.onPrimaryClick(...arguments);
      }
    },

    /**
     * Handles graph item selection
     */
    onSelection() {
      this.attrs.onSelection(...arguments);
    },

    /**
     * Shows/Hides the graph legend
     */
    onToggle() {
      this.toggleProperty('showGraphLegend');
    },

    /**
     * Scrolls to the appropriate tab on click
     */
    scrollToSection() {
      later(() => {
        $('#root-cause-analysis').get(0).scrollIntoView();
      });
    }
  }
});
