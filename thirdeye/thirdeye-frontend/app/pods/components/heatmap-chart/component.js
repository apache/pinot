import Ember from 'ember';
import _ from 'lodash';
import d3 from 'd3';


const { get } = Ember;

// TODO: move to utils file
const getBackgroundColor = function (factor = 0) {
  const opacity = Math.abs(factor / 25);
  const color = factor > 0 ? '0,0,234' : '234,0,0';

  return `rgba(${color},${opacity})`;
};

// TODO: move to utils file
const getTextColor = function (factor = 0) {
  const opacity = Math.abs(factor / 25);

  return opacity < 0.5 ? '#000000' : '#ffffff';
};

export default Ember.Component.extend({
  current: { // dimensions
    country: { // dimension namespace (dimNameObj)
      us: 100, // dimension value
      cn: 100,
      ca: 150
    },
    browser: {
      chrome: 250,
      firefox: 100
    }
  },

  baseline: {
    country: {
      us: 90,
      cn: 90,
      ca: 70
    },
    browser: {
      chrome: 180,
      firefox: 70
    }
  },

  mode: null, // 'change', 'contributionDiff', 'contributionToDiff'

  rollup: 10,

  /**
   * Bubbles the click up to the parent component
   * @param {String[]} subdimension
   * @return {Function}
   */
  heatmapClickHandler(subdimension) {
    const callback = this.attrs.onHeatmapClick;
    const {
      name,
      dimension
    } = subdimension;
    if (!callback) { return; }

    callback([dimension, name]);
  },

  /**
   * Destroys the heatmap svgs
   * @return {undefined}
   */
  _cleanUp() {
    d3.select('.dimension-heatmap').selectAll('svg').remove();
  },

  /**
   * Builds heatmap
   */
  _buildHeatmap() {
    const {
      scores, sizes
    } = this.getProperties('scores', 'sizes');

    const dimensions = Object.keys(sizes);
    if (!dimensions.length) { return; }

    dimensions.forEach((dimension) => {
      const dimensionPlaceHolderId = `#${dimension}-heatmap-placeholder`;
      const data = sizes[dimension];
      const children = Object.entries(data)
        .filter(([, size]) => size)
        .map(([name, size]) => {
          return {
            name,
            value: size,
            size: size,
            actualValue: scores[dimension][name],
            dimension
          };
        });

      const domElem = Ember.$(dimensionPlaceHolderId);
      const height = domElem.height();
      const width = domElem.width();
      const treeMap = d3.layout.treemap()
        .size([width, height])
        .sort((a, b) => a.size - b.size);

      const div = d3.select(dimensionPlaceHolderId)
        .attr('class', 'heatmap')
        .append('svg:svg')
        .attr('width', width)
        .attr('height', height)
        .append('svg:g')
        .attr('transform', 'translate(.5,.5)');

      const nodes = treeMap
        .nodes({ name: '0', children: children })
        .filter((node) => !node.children);

      this._createCell(div, nodes);
    });
  },

  /**
   * Builds an individual cell based on the provided div and nodes
   */
  _createCell(div, nodes) {
    const cell = div.selectAll('g')
      .data(nodes)
      .enter()
      .append('svg:g')
      .attr('class', 'cell')
      .attr('transform', d => `translate(${d.x},${d.y})`);

    // tooltip
    cell.on('mousemove', (d) => {
      const tooltipWidth = 200;
      const xPosition = d3.event.pageX - (tooltipWidth + 20);
      const yPosition = d3.event.pageY + 5;

      d3.select('#tooltip')
        .style('left', xPosition + 'px')
        .style('top', yPosition + 'px');
      d3.select('#tooltip #heading')
        .text(d.name);
      d3.select('#tooltip #currentValue')
        .text(d.actualValue);
      d3.select('#tooltip').classed('hidden', false);
    }).on('mouseout', function () {
      d3.select('#tooltip').classed('hidden', true);
    });

    // colored background
    cell.append('svg:rect')
      .attr('width', d => Math.max(d.dx - 1, 0))
      .attr('height', d => Math.max(d.dy - 1, 0))
      .style('fill', d => getBackgroundColor(d.actualValue));

    // colored text
    cell.append('svg:text')
      .attr('x', d => (d.dx / 2))
      .attr('y', d => (d.dy / 2))
      .attr('dy', '.35em')
      .attr('text-anchor', 'middle')
      .text((d) => {
        const text = `${d.name} (${d.actualValue})`;

        //each character takes up 7 pixels on an average
        const estimatedTextLength = text.length * 7;
        if (estimatedTextLength > d.dx) {
          return text.substring(0, d.dx / 7) + '..';
        } else {
          return text;
        }
      })
      .style('fill', d => getTextColor(d.actualValue));

    cell.on('click', get(this, 'heatmapClickHandler').bind(this));
  },

  init() {
    this._super(...arguments);
    this.set('mode', 'change');
  },

  didUpdateAttrs() {
    this._super(...arguments);

    this._cleanUp();
  },

  willRender() {
    this._super(...arguments);

    this._cleanUp();
  },

  didRender() {
    this._super(...arguments);

    this._buildHeatmap();
  },

  _dataRollup: Ember.computed(
    'current',
    'baseline',
    'rollup',
    function () {
      const { current, baseline, rollup } =
        this.getProperties('current', 'baseline', 'rollup');

      // collect all dimension names
      const dimNames = new Set(Object.keys(current).concat(Object.keys(baseline)));

      // collect all dimension values for all dimension names
      const dimValues = {};
      [...dimNames].forEach(n => dimValues[n] = new Set());
      [...dimNames].filter(n => n in current).forEach(n => Object.keys(current[n]).forEach(v => dimValues[n].add(v)));
      [...dimNames].filter(n => n in baseline).forEach(n => Object.keys(baseline[n]).forEach(v => dimValues[n].add(v)));

      const values = {};
      [...dimNames].forEach(n => {
        let curr = current[n] || {};
        let base = baseline[n] || {};
        let dimVals = dimValues[n] || new Set();

        // conditional rollup
        if (rollup > 0 && Object.keys(curr).length >= rollup) {
          const topk = new Set(this._makeTopK(curr, rollup - 1));
          curr = this._makeRollup(curr, topk, 'OTHER');
          base = this._makeRollup(base, topk, 'OTHER');
          dimVals = new Set(['OTHER', ...topk]);
        }

        values[n] = {};
        [...dimVals].forEach(v => {
          values[n][v] = {
            current: curr[v] || 0,
            baseline: base[v] || 0
          };
        });
      });

      return values;
    }
  ),

  scores: Ember.computed(
    '_dataRollup',
    'mode',
    function () {
      const { _dataRollup: data, mode } = this.getProperties('_dataRollup', 'mode');

      const transformation = this._makeTransformation(mode);

      const scores = {};
      Object.keys(data).forEach(n => {
        scores[n] = {};
        Object.keys(data[n]).forEach(v => {
          const curr = data[n][v].current;
          const base = data[n][v].baseline;
          const currTotal = this._makeSum(data[n], (d) => d.current);
          const baseTotal = this._makeSum(data[n], (d) => d.baseline);

          scores[n][v] = Math.round(transformation(curr, base, currTotal, baseTotal) * 10000) / 100.0; // percent, 2 commas
        });
      });

      return scores;
    }
  ),

  /**
   * Current Contribution fraction
   */
  sizes: Ember.computed(
    '_dataRollup',
    function () {
      const { _dataRollup: data } = this.getProperties('_dataRollup');

      const contributions = {};
      Object.keys(data).forEach(n => {
        contributions[n] = {};
        Object.keys(data[n]).forEach(v => {
          const curr = data[n][v].current;
          const currTotal = this._makeSum(data[n], (d) => d.current);

          contributions[n][v] = Math.round(1.0 * curr / currTotal * 10000) / 100.0; // percent, 2 commas
        });
      });

      return contributions;
    }
  ),

  _makeTransformation(mode) {
    switch (mode) {
      case 'change':
        return (curr, base, currTotal, baseTotal) => curr / base - 1;
      case 'contributionDiff':
        return (curr, base, currTotal, baseTotal) => curr / currTotal - base / baseTotal;
      case 'contributionToDiff':
        return (curr, base, currTotal, baseTotal) => (curr - base) / baseTotal;
    }
    return (curr, base, currTotal, baseTotal) => 0;
  },

  _makeRollup(dimNameObj, topk, otherValue) {
    if (!dimNameObj) {
      return dimNameObj;
    }
    const rollup = {};
    [...topk].forEach(v => rollup[v] = dimNameObj[v]);

    const sumOther = this._makeSumOther(dimNameObj, topk);
    rollup[otherValue] = sumOther;

    return rollup;
  },

  _makeSum(dimNameObj, funcExtract) {
    if (!dimNameObj) {
      return 0;
    }
    return Object.values(dimNameObj).reduce((agg, x) => agg + funcExtract(x), 0);
  },

  _makeSumOther(dimNameObj, topk) {
    if (!dimNameObj) {
      return 0;
    }
    return Object.keys(dimNameObj).filter(v => !topk.has(v)).map(v => dimNameObj[v]).reduce((agg, x) => agg + x, 0);
  },

  _makeTopK(dimNameObj, k) {
    if (!dimNameObj) {
      return [];
    }
    const tuples = Object.keys(dimNameObj).map(v => [-dimNameObj[v], v]).sort();
    const dimValues = _.slice(tuples, 0, k).map(t => t[1]);
    return dimValues;
  }
});
