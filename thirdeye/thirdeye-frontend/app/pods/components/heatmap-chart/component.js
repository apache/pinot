import Ember from 'ember';
import _ from 'lodash';
import d3 from 'd3';


const { get } = Ember;

// TODO: move to utils file
const getBackgroundColor = function (factor = 0) {
  const opacity = Math.abs(factor / 0.25);
  const color = factor > 0 ? '0,0,234' : '234,0,0';

  return `rgba(${color},${opacity})`;
};

// TODO: move to utils file
const getTextColor = function (factor = 0) {
  const opacity = Math.abs(factor / 0.25);

  return opacity < 0.5 ? '#000000' : '#ffffff';
};

export default Ember.Component.extend({
  cells: null, // {}

  /**
   * Bubbles the click up to the parent component
   * @param {String[]} subdimension
   * @return {Function}
   */
  heatmapClickHandler(subdimension) {
    const callback = this.attrs.onHeatmapClick;
    const {
      role,
      dimName,
      dimValue
    } = subdimension;

    if (!callback) { return; }

    callback(role, dimName, dimValue);
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
      cells
    } = this.getProperties('cells');

    const dimensions = Object.keys(cells);
    if (!dimensions.length) { return; }

    dimensions.forEach((dimension) => {
      const dimensionPlaceHolderId = `#${dimension}-heatmap-placeholder`;
      const children = cells[dimension]
        .filter(({ size }) => size)
        .map(({ label, size, value, dimName, dimValue, index, role }) => {
          return {
            label,
            value: size,
            size: size,
            actualValue: value,
            dimName,
            dimValue,
            role,
            index
          };
        });

      const domElem = Ember.$(dimensionPlaceHolderId);
      const height = domElem.height();
      const width = domElem.width();
      const treeMap = d3.layout.treemap()
        .size([width, height])
        .sort((a, b) => b.index - a.index);

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
        const text = d.label || '';

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
