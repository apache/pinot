import $ from 'jquery';
import Component from '@ember/component';
import { get } from '@ember/object';
import d3 from 'd3';

// TODO: move to utils file
const getBackgroundColor = function (factor = 0, inverse = false) {
  if (Number.isNaN(factor)) {
    return 'rgba(0,0,0,0)';
  }

  if (inverse) {
    factor *= -1;
  }

  const opacity = Math.min(Math.abs(factor / 0.25), 1.0);
  const color = factor > 0 ? '0,0,234' : '234,0,0';

  return `rgba(${color},${opacity})`;
};

// TODO: move to utils file
const getTextColor = function (factor = 0, inverse = false) {
  if (Number.isNaN(factor)) {
    return 'rgba(0,0,0,255)';
  }

  if (inverse) {
    factor *= -1;
  }

  const opacity = Math.min(Math.abs(factor / 0.25), 1.0);

  return opacity < 0.5 ? '#000000' : '#ffffff';
};

export default Component.extend({
  cells: null, // {}

  /**
   * ID Selector of the tooltip
   * (in application/template.hbs)
   */
  tooltipId: '#heatmap-tooltip',

  /**
   * Bubbles the click up to the parent component
   * @param {String[]} subdimension
   * @return {Function}
   */
  includeHandler(subdimension) {
    const onInclude = get(this, 'onInclude');
    const { role, dimName, dimValue } = subdimension.data || {};

    if (!onInclude) {
      return;
    }

    onInclude(role, dimName, dimValue);
  },

  /**
   * Bubbles the right-click up to the parent component
   * @param {String[]} subdimension
   * @return {Function}
   */
  excludeHandler(subdimension) {
    d3.event.preventDefault();

    const onExclude = get(this, 'onExclude');
    const { role, dimName, dimValue } = subdimension.data || {};

    if (!onExclude) {
      return;
    }

    onExclude(role, dimName, dimValue);
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
    const { cells, tooltipId } = this.getProperties('cells', 'tooltipId');

    const dimensions = Object.keys(cells);
    if (!dimensions.length) {
      return;
    }

    dimensions.forEach((dimension) => {
      const dimensionPlaceHolderId = `#${dimension}-heatmap-placeholder`;
      const children = cells[dimension]
        .filter(({ size }) => size)
        .map((cell) => {
          const { size, value } = cell;
          return Object.assign({}, cell, {
            value: size,
            size: size,
            actualValue: value
          });
        });

      const domElem = $(dimensionPlaceHolderId);
      const height = domElem.height();
      const width = domElem.width();

      // initialize treemap
      const treeMap = (data) => {
        const formateData = d3
          .hierarchy(data)
          .sum((d) => d.value)
          .sort((a, b) => b.index - a.index); // sorted by index
        return d3
          .treemap() // with given size
          .size([width, height])(formateData);
      };

      const div = d3
        .select(dimensionPlaceHolderId)
        .attr('class', 'heatmap')
        .append('svg:svg')
        .attr('width', width)
        .attr('height', height)
        .append('svg:g')
        .attr('transform', 'translate(.5,.5)');

      // Pass root with children
      const nodes = treeMap({ name: '0', children: children })
        // specify children of treemap
        .children // only nodes which don't have children
        .filter((node) => !node.children);
      this._createCell(div, nodes, tooltipId);
    });
  },

  /**
   * Builds an individual cell based on the provided div and nodes
   */
  _createCell(div, nodes, tooltipId) {
    const cell = div
      .selectAll('g')
      .data(nodes)
      .enter()
      .append('svg:g')
      .attr('class', 'heatmap-chart__cell')
      .attr('transform', (d) => `translate(${d.x0},${d.y0})`);

    // tooltip
    cell
      .on('mousemove', (d) => {
        if (d && d.data && d.data.role !== 'value') {
          return;
        }

        const tooltipWidth = 200;
        const xPosition = d3.event.pageX - (tooltipWidth + 20);
        const yPosition = d3.event.pageY + 5;

        d3.select(`${tooltipId}`)
          .style('left', xPosition + 'px')
          .style('top', yPosition + 'px');

        Object.keys(d.data).forEach((key) => {
          d3.select(`${tooltipId} #${key}`).text(d.data[key]);
        });

        d3.select(`${tooltipId}`).classed('hidden', false);
      })
      .on('mouseout', function () {
        d3.select(`${tooltipId}`).classed('hidden', true);
      })
      .on('mousedown', function () {
        d3.select(`${tooltipId}`).classed('hidden', true);
      });

    // colored background
    cell
      .append('svg:rect')
      .attr('width', (d) => Math.max(d.x1 - d.x0 - 1, 0))
      .attr('height', (d) => Math.max(d.y1 - d.y0 - 1, 0))
      .style('fill', (d) => getBackgroundColor(d.data.actualValue, d.data.inverse));

    // colored text
    cell
      .append('svg:text')
      .attr('x', (d) => (d.x1 - d.x0) / 2)
      .attr('y', (d) => (d.y1 - d.y0) / 2)
      .attr('dy', '.35em')
      .attr('text-anchor', 'middle')
      .text((d) => {
        const text = d.data.label || '';

        //each character takes up 7 pixels on an average
        const estimatedTextLength = text.length * 7;
        if (estimatedTextLength > d.x1 - d.x0) {
          return text.substring(0, (d.x1 - d.x0) / 7) + '..';
        } else {
          return text;
        }
      })
      .style('fill', (d) => {
        // return default color for icons
        if (d.data.role !== 'value') {
          return 'rgba(0,0,0,0.45)';
        }
        return getTextColor(d.data.actualValue, d.data.inverse);
      });

    cell.on('click', get(this, 'includeHandler').bind(this));
    cell.on('contextmenu', get(this, 'excludeHandler').bind(this));
  },

  init() {
    this._super(...arguments);
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
  }
});
