import { debounce, later } from "@ember/runloop";
import Component from "@ember/component";
import c3 from "c3";
import d3 from "d3";
import _ from "lodash";
import moment from "moment";

export default Component.extend({
  tagName: "div",
  classNames: ["timeseries-chart"],

  // internal
  _chart: null,
  _seriesCache: null,

  // external
  series: {
    example_series: {
      timestamps: [0, 1, 2, 5, 6],
      values: [10, 10, 5, 27, 28],
      type: "line", // 'scatter', 'region'
      color: "green",
    },
  },

  /**
   * Ids of the entity to focus
   * @type {Set}
   */
  focusedIds: new Set(),

  /**
   * default height property for the chart
   * May be overridden
   */
  height: {
    height: 400,
  },

  tooltip: {
    format: {
      title: (d) => moment(d).format("MM/DD hh:mm a"),
      value: (val) => d3.format(".3s")(val),
      name: (name) => {
        if (name === "Upper and lower bound") {
          return "Upper bound";
        } else if (name === "lowerBound") {
          return "Lower bound";
        }
        return name;
      },
    },
  },

  legend: {
    hide: ["lowerBound", "old-anomaly-edges", "new-anomaly-edges"],
  },

  axis: {
    y: {
      show: true,
    },
    y2: {
      show: false,
    },
    x: {
      type: "timeseries",
      show: true,
      tick: {
        count: 5,
        format: "%Y-%m-%d",
      },
    },
  },

  /**
   * Converts color strings into their rgba equivalent
   */
  colorMapping: {
    blue: "#0091CA",
    green: "#469A1F",
    red: "#FF2C33",
    purple: "#827BE9",
    orange: "#E55800",
    teal: "#0E95A0",
    pink: "#FF1B90",
  },

  subchart: {
    // on init only
    show: true,
  },

  zoom: {
    // on init only
    enabled: true,
    onzoom: null,
  },

  point: {
    // on init only
    show: true,
  },

  line: {
    // on init only
    connectNull: false,
  },

  id: "timeseries-chart",

  _makeDiffConfig() {
    const cache = this.get("_seriesCache") || {};
    const series = this.get("series") || {};
    const colorMapping = this.get("colorMapping");
    const { axis, legend, tooltip, focusedIds } = this.getProperties(
      "axis",
      "legend",
      "tooltip",
      "focusedIds"
    );

    const seriesKeys = Object.keys(series).sort();

    const addedKeys = seriesKeys.filter((sid) => !cache[sid]);
    const changedKeys = seriesKeys.filter(
      (sid) => cache[sid] && !_.isEqual(cache[sid], series[sid])
    );
    const deletedKeys = Object.keys(cache).filter((sid) => !series[sid]);
    const regionKeys = seriesKeys.filter(
      (sid) => series[sid] && series[sid].type == "region"
    );
    // keys containing '-region' should not appear in the graph legend.
    const noLegendKeys = seriesKeys.filter((sid) => sid.includes("-region"));

    const regions = regionKeys.map((sid) => {
      const t = series[sid].timestamps;
      let region = { start: t[0], end: t[t.length - 1] };

      if ("color" in series[sid]) {
        region.class = `c3-region--${series[sid].color}`;
      }

      return region;
    });

    const unloadKeys = deletedKeys.concat(noLegendKeys);
    const unload = unloadKeys.concat(
      unloadKeys.map((sid) => `${sid}-timestamps`)
    );

    const loadKeys = addedKeys
      .concat(changedKeys)
      .filter((sid) => !noLegendKeys.includes(sid));
    const xs = {};
    loadKeys.forEach((sid) => (xs[sid] = `${sid}-timestamps`));

    const values = loadKeys.map((sid) => [sid].concat(series[sid].values));

    const timestamps = loadKeys.map((sid) =>
      [`${sid}-timestamps`].concat(series[sid].timestamps)
    );

    const columns = values.concat(timestamps);

    const colors = {};
    loadKeys
      .filter((sid) => series[sid].color)
      .forEach((sid) => (colors[sid] = colorMapping[series[sid].color]));

    const types = {};
    loadKeys
      .filter((sid) => series[sid].type)
      .forEach((sid) => (types[sid] = series[sid].type));

    const axes = {};
    loadKeys
      .filter((sid) => "axis" in series[sid])
      .forEach((sid) => (axes[sid] = series[sid].axis));
    // keep the lower bound line in graph but remove in from the legend
    legend.hide = ["lowerBound", "old-anomaly-edges", "new-anomaly-edges"];
    const config = {
      unload,
      xs,
      columns,
      types,
      regions,
      tooltip,
      focusedIds,
      colors,
      axis,
      axes,
      legend,
    };
    return config;
  },

  _makeAxisRange(axis) {
    const range = { min: {}, max: {} };
    Object.keys(axis)
      .filter((key) => "min" in axis[key])
      .forEach((key) => (range["min"][key] = axis[key]["min"]));
    Object.keys(axis)
      .filter((key) => "max" in axis[key])
      .forEach((key) => (range["max"][key] = axis[key]["max"]));
    return range;
  },

  _updateCache() {
    // debounce: do not trigger if chart object already destroyed
    if (this.isDestroyed) {
      return;
    }

    const series = this.get("series") || {};
    this.set("_seriesCache", _.cloneDeep(series));
  },

  /**
   * Updates the focused entity on the chart
   */
  _updateFocusedIds: function () {
    const ids = this.get("focusedIds");

    if (!_.isEmpty(ids)) {
      this._focus([...ids]);
    } else {
      this._revert();
    }
  },

  /**
   * Focuses the entity associated with the provided id
   * @private
   * @param {String} id
   */
  _focus(id) {
    this.get("_chart").focus(id);
  },

  /**
   * Reverts the chart back to its original state
   * @private
   */
  _revert() {
    this.get("_chart").revert();
  },

  _updateChart() {
    const diffConfig = this._makeDiffConfig();
    const chart = this.get("_chart");
    chart.regions(diffConfig.regions);
    chart.axis.range(this._makeAxisRange(diffConfig.axis));
    chart.unzoom();
    chart.load(diffConfig);
    this._updateCache();
  },

  _shadeBounds() {
    const parentElement = this.api.internal.config.bindto;
    d3.select(parentElement).select(".confidence-bounds").remove();
    d3.select(parentElement).select(".sub-confidence-bounds").remove();
    d3.select(parentElement)
      .select(".timeseries-graph__slider-circle")
      .remove();
    d3.select(parentElement)
      .selectAll("timeseries-graph__slider-line")
      .remove();
    const chart = this.api;
    if (
      chart &&
      chart.legend &&
      chart.internal &&
      chart.internal.data &&
      chart.internal.data.targets
    ) {
      if (chart.internal.data.targets.length > 24) {
        chart.legend.hide();
      }
    }
    // key is 'Upper and lower bound' because we delete the lowerBound key for the legend.
    if (
      chart &&
      chart.internal &&
      chart.internal.data &&
      chart.internal.data.xs &&
      Array.isArray(chart.internal.data.xs["Upper and lower bound"])
    ) {
      const indices = d3.range(
        chart.internal.data.xs["Upper and lower bound"].length
      );
      const yscale = chart.internal.y;
      const xscale = chart.internal.x;
      const yscaleSub = chart.internal.subY;
      const xscaleSub = chart.internal.subX;
      const xVals = chart.internal.data.xs["Upper and lower bound"];
      let upperBoundVals = chart.internal.data.targets.find((target) => {
        return target.id === "Upper and lower bound";
      });
      let lowerBoundVals = chart.internal.data.targets.find((target) => {
        return target.id === "lowerBound";
      });

      if (upperBoundVals && lowerBoundVals) {
        upperBoundVals = upperBoundVals.values.map((e) => e.value);
        lowerBoundVals = lowerBoundVals.values.map((e) => e.value);
        // If all upper bound vals are null, we assume that there is only a lower bound
        if (upperBoundVals.every((val) => val === null)) {
          let currentVals = chart.internal.data.targets.find((target) => {
            return target.id === "Current";
          });
          if (currentVals) {
            currentVals = currentVals.values.map((e) => e.value);
            const currentMax = Math.max(...currentVals);
            upperBoundVals = upperBoundVals.map(() => 2 * currentMax);
          }
        }
        const area_main = d3
          .area()
          .curve(d3.curveLinear)
          .x((d) => xscale(xVals[d]))
          .y0((d) => yscale(lowerBoundVals[d]))
          .y1((d) => yscale(upperBoundVals[d]));

        const area_sub = d3
          .area()
          .curve(d3.curveLinear)
          .x((d) => xscaleSub(xVals[d]))
          .y0((d) => yscaleSub(lowerBoundVals[d]))
          .y1((d) => yscaleSub(upperBoundVals[d]));

        let i = 0;
        const bothCharts = d3.select(parentElement).selectAll(".c3-chart-bars");
        bothCharts.each(function () {
          if (i === 0 && this) {
            d3.select(this)
              .insert("path")
              .datum(indices)
              .attr("class", "confidence-bounds")
              .attr("d", area_main);
          } else if (i === 1 && this) {
            d3.select(this)
              .insert("path")
              .datum(indices)
              .attr("class", "sub-confidence-bounds")
              .attr("d", area_sub);
          }
          i++;
        });
      }
    }
    // add resize buttons after shading bounds
    const resizeButtons = d3.selectAll(".resize");

    resizeButtons
      .append("circle")
      .attr("class", "timeseries-graph__slider-circle")
      .attr("cx", 0)
      .attr("cy", 30)
      .attr("r", 10)
      .attr("fill", "#0091CA");
    resizeButtons
      .append("line")
      .attr("class", "timeseries-graph__slider-line")
      .attr("x1", 0)
      .attr("y1", 27)
      .attr("x2", 0)
      .attr("y2", 33);

    resizeButtons
      .append("line")
      .attr("class", "timeseries-graph__slider-line")
      .attr("x1", -5)
      .attr("y1", 27)
      .attr("x2", -5)
      .attr("y2", 33);

    resizeButtons
      .append("line")
      .attr("class", "timeseries-graph__slider-line")
      .attr("x1", 5)
      .attr("y1", 27)
      .attr("x2", 5)
      .attr("y2", 33);
  },

  didUpdateAttrs() {
    this._super(...arguments);
    const series = this.get("series") || {};
    const cache = this.get("cache") || {};

    this._updateFocusedIds();

    if (!_.isEqual(series, cache)) {
      debounce(this, this._updateChart, 300);
    }
  },

  didRender() {
    this._super(...arguments);

    later(() => {
      this.notifyPhantomJS();
    });
  },

  /**
   * Checks if the page is being viewed from phantomJS
   * and notifies it that the page is rendered and ready
   * for a screenshot
   */
  notifyPhantomJS() {
    if (typeof window.callPhantom === "function") {
      window.callPhantom({ message: "ready" });
    }
  },

  didInsertElement() {
    this._super(...arguments);

    const diffConfig = this._makeDiffConfig();
    const config = {};
    config.bindto = this.get("element");
    config.data = {
      xs: diffConfig.xs,
      columns: diffConfig.columns,
      types: diffConfig.types,
      colors: diffConfig.colors,
      axes: diffConfig.axes,
    };
    config.axis = diffConfig.axis;
    config.regions = diffConfig.regions;
    config.tooltip = diffConfig.tooltip;
    config.focusedIds = diffConfig.focusedIds;
    config.legend = diffConfig.legend;
    config.subchart = this.get("subchart");
    config.zoom = this.get("zoom");
    config.size = this.get("height");
    config.point = this.get("point");
    config.line = this.get("line");
    config.id = this.get("id");
    config.onrendered = this.get("_shadeBounds");

    const chart = c3.generate(config);
    this.set("_chart", chart);

    this._updateCache();
  },
});
