import * as d3 from "d3";
import _ from "lodash";
import {
  LayerBase,
  MZPoint,
  PointLike,
} from "./layers";
import { uuidv4 } from "../util";

export const defaultMargins = {
  top: 10,
  right: 30,
  bottom: 50,
  left: 90,
};

export const defaultWidth = 1200 - defaultMargins.left - defaultMargins.right;
export const defaultHeight = 500 - defaultMargins.top - defaultMargins.bottom;

export const DEFAULT_COLOR_CYCLE = [
  "steelblue",
  "blueviolet",
  "lightseagreen",
  "midnightblue",
  "limegreen",
  "goldenrod",
  "firebrick",
  "crimson",
];

export class ColorCycle {
  colors: string[];
  index: number;
  length: number;

  constructor(colors: string[]) {
    if (!colors || colors.length === 0) {
      colors = Array.from(DEFAULT_COLOR_CYCLE);
    }
    this.colors = colors;
    this.index = 0;
    this.length = this.colors.length;
  }

  nextColor() {
    if (this.index >= this.length) {
      this.index = 0;
    }
    let color = this.colors[this.index];
    this.index++;
    return color;
  }

  reset() {
    this.index = 0;
  }
}

export interface ScanRange {
  lowerBound: number;
  upperBound: number;
}

export interface DimensionLabels {
  xLabel: string;
  yLabel: string;
}

export interface CanvasUpdateEvent<T extends PointLike> {
  canvas: MSCanvasBase<T>;
}

export class MSCanvasBase<T extends PointLike> {
  spectrumID: any;

  containerSelector: string;
  outer: d3.Selection<SVGSVGElement, PointLike, HTMLElement, any> | null;
  container: d3.Selection<SVGGElement, PointLike, HTMLElement, any> | null;
  layers: LayerBase<T>[];
  extentCoordinateInterval: [number, number];
  scanRange: ScanRange | null;

  width: number;
  height: number;
  margins: any;
  dimensionLabels: DimensionLabels;
  eventHandlers: Function[];
  emitEvent: Function;

  xScale: d3.ScaleLinear<number, number> | null;
  yScale: d3.ScaleLinear<number, number> | null;
  xAxis: d3.Selection<SVGGElement, PointLike, HTMLElement, any> | null;
  yAxis: d3.Selection<SVGGElement, PointLike, HTMLElement, any> | null;

  xLabel: d3.Selection<SVGTextElement, PointLike, HTMLElement, any> | null;
  yLabel: d3.Selection<SVGTextElement, PointLike, HTMLElement, any> | null;

  colorCycle: ColorCycle;

  idledTimeout: any | null;
  brush: any | null;
  clip: d3.Selection<d3.BaseType, PointLike, HTMLElement, any> | null;

  pointerXLabel: d3.Selection<
    SVGTextElement,
    PointLike,
    HTMLElement,
    any
  > | null;
  pointerYLabel: d3.Selection<
    SVGTextElement,
    PointLike,
    HTMLElement,
    any
  > | null;

  constructor(
    containerSelector: string,
    width: number,
    height: number,
    margins: any,
    colors: string[],
    id: any,
    scanRange?: ScanRange | null,
    dimensionLabels?: DimensionLabels | null
  ) {
    this.containerSelector = containerSelector;
    this.spectrumID = id;
    this.width = width ?? defaultWidth;
    this.height = height ?? defaultHeight;
    this.margins = margins;
    this.scanRange = scanRange || { lowerBound: 80.0, upperBound: 2000.0 };

    if (this.margins === undefined) {
      this.margins = defaultMargins;
    }

    this.outer = null;
    this.container = null;
    this.xScale = null;
    this.yScale = null;
    this.xAxis = null;
    this.yAxis = null;

    this.xLabel = null;
    this.yLabel = null;

    this.dimensionLabels = dimensionLabels
      ? dimensionLabels
      : { xLabel: "m/z", yLabel: "Relative Intensity" };

    this.pointerXLabel = null;
    this.pointerYLabel = null;

    this.clip = null;

    this.brush = null;

    this.idledTimeout = null;

    this.layers = [];
    this.colorCycle = new ColorCycle(colors);
    this.extentCoordinateInterval = [0, 0];
    this.eventHandlers = [];
    this.emitEvent = _.debounce(this._emitUpdateEvent, 5)
    this.updateCoordinateInterval();
  }

  addLayer(layer: LayerBase<T>) {
    this.layers.push(layer);
    if (this.container) {
      layer.initArtist(this);
      this.render();
    }
  }

  addLayers(layers: LayerBase<T>[]) {
    for (let layer of layers) {
      this.layers.push(layer);
      if (this.container) {
        layer.initArtist(this);
      }
    }
    this.updateCoordinateInterval();
    if (this.container) {
      this.render();
    }
  }

  updateCoordinateInterval() {
    this.extentCoordinateInterval = [
      this.minCoordinate(),
      this.maxCoordinate(),
    ];
  }

  clear() {
    this.remove();
    this.layers = [];
    this.extentCoordinateInterval = [0, 0];
    this.container?.remove();
    this.outer?.remove();
    this.outer = null;
    this.container = null;
  }

  minXDim() {
    if (this.layers.length === 0) {
      return 0;
    }
    const dim = Math.max(
      0,
      Math.min.apply(
        null,
        this.layers
          .filter((d) => d.length > 0)
          .map((d) => d.minX())
          .filter((value) => value !== undefined && !Number.isNaN(value))
      )
    );
    return dim - 50 > 0 ? dim - 50 : 0;
  }

  minCoordinate() {

    return Math.min(this.scanRange?.lowerBound || Infinity, this.minXDim());
  }

  maxXDim() {
    if (this.layers.length === 0) {
      return 0;
    }
    return Math.max.apply(
      null,
      this.layers
        .map((d) => d.maxX())
        .filter((value) => value !== undefined && !Number.isNaN(value))
    );
  }

  maxCoordinate() {

    const maxMz = this.maxXDim();
    const upperBound = this.scanRange?.upperBound || Infinity;
    const padded = Math.min(maxMz * 1.1, maxMz + 50.0);
    return Math.min(padded, upperBound);
  }

  minYDim() {
    if (this.layers.length === 0) {
      return 0;
    }
    return Math.min.apply(
      null,
      this.layers
        .map((d) => d.minY())
        .filter((value) => value !== undefined && !Number.isNaN(value))
    );
  }

  maxYDim() {
    if (this.layers.length === 0) {
      return 0;
    }
    return Math.max.apply(
      null,
      this.layers.map((d) => d.maxY())
    );
  }

  maxYDimBetween(low: number, high: number) {
    return Math.max.apply(
      null,
      this.layers.map((layer) => layer.between(low, high).maxY())
    );
  }

  minYDimBetween(low: number, high: number) {
    return Math.min.apply(
      null,
      this.layers.map((layer) => layer.between(low, high).minY())
    );
  }

  initContainer() {
    if (!this.container) {
      // Initialize the SVG container for the first time. Do not do this again because this element is
      // not removed by its own .remove()
      this.outer = d3
        .select<SVGElement, PointLike>(this.containerSelector)
        .append("svg")
        .attr("width", this.width + this.margins.left + this.margins.right)
        .attr("height", this.height + this.margins.top + this.margins.bottom);
      this.container = this.outer
        .append("g")
        .attr(
          "transform",
          `translate(${this.margins.left}, ${this.margins.right})`
        )
        .attr("id", uuidv4());
    }
    const minY = this.minYDim();
    const maxY = this.maxYDim();
    // Initialize the supporting properties
    this.xScale = d3
      .scaleLinear()
      .domain([this.minCoordinate(), this.maxCoordinate()])
      .range([0, this.width]);
    this.yScale = d3
      .scaleLinear()
      .domain([minY * 0.75, maxY * 1.25])
      .range([this.height, 0]);
    this.xAxis = this.container
      .append("g")
      .attr("transform", `translate(0, ${this.height})`)
      .call(d3.axisBottom(this.xScale));

    this.yAxis = this.container.append("g").call(d3.axisLeft(this.yScale));

    this.clip = this.container
      .append("defs")
      .append("svg:clipPath")
      .attr("id", "clip")
      .append("svg:rect")
      .attr("width", this.width)
      .attr("height", this.height)
      .attr("x", 0)
      .attr("y", 0);

    this.defineZoom()

    this.container.on("dblclick", () => {
      this.resetZoom();
    });

    this.yLabel = this.container
      .append("text")
      .attr("transform", "rotate(-90)")
      .attr("y", 0 - this.margins.left)
      .attr("x", 0 - this.height / 2)
      .attr("dy", "1em")
      .style("text-anchor", "middle")
      .attr("class", "axis-label")
      .text(this.dimensionLabels.yLabel);

    this.xLabel = this.container
      .append("text")
      .attr(
        "transform",
        "translate(" +
          this.width / 2 +
          " ," +
          (this.height + this.margins.top + 20) +
          ")"
      )
      .style("text-anchor", "middle")
      .attr("class", "axis-label")
      .text(this.dimensionLabels.xLabel);

    this.createMouseLabel()
    this.configureMouseLabel();
  }

  defineZoom() {
    this.brush = d3
      .brushX<any>()
      .extent([
        [0, 0],
        [this.width, this.height],
      ])
      // throttle this call to avoid one call to updateChart per layer
      .on(
        "end",
        _.throttle(() => this.updateChart(), 200, { trailing: false })
      );
  }

  createMouseLabel() {
    if (this.container === null) return
    this.pointerXLabel = this.container
      .append("text")
      .attr(
        "transform",
        `translate(${this.width * 0.01},${this.height * 0.02})`
      )
      .style("text-anchor", "left")
      .attr("class", "cursor-label")
      .text("");

    this.pointerYLabel = this.container
      .append("text")
      .attr(
        "transform",
        `translate(${this.width * 0.01},${this.height * 0.06})`
      )
      .style("text-anchor", "left")
      .attr("class", "cursor-label")
      .text("");
  }

  configureMouseLabel() {
    let self = this;
    if (!this.container) return;
    this.container.on("mousemove", function () {
      // Binds the coordinates within `this`, the component containing the event
      let mouse = d3.mouse(this);
      const dimLabels = self.dimensionLabels;
      requestAnimationFrame((_timestamp) => {
        if (!self.xScale || !self.yScale)
          throw new Error("Uninitialized scales");
        let mzLabel = self.xScale.invert(mouse[0]);
        let intensityLabel = self.yScale.invert(mouse[1]);
        self.pointerXLabel?.text(
          `${dimLabels.xLabel} = ${mzLabel > 0 ? mzLabel.toFixed(3) : "-"}`
        );
        self.pointerYLabel?.text(
          `Int. = ${intensityLabel > 0 ? intensityLabel.toExponential(2) : "-"}`
        );
        for (let layer of self.layers) {
          layer.onHover(self, { mz: mzLabel, intensity: intensityLabel });
        }
      });
    });
  }

  resetZoom() {
    console.log("Resetting Canvas...");
    if (!this.xScale || !this.yScale) {
      throw new Error("Uninitialized scales");
    }
    this.setExtentByCoordinate(undefined, undefined);
  }

  remove() {
    // Remove all elements from the DOM
    this.yAxis?.remove();
    this.xAxis?.remove();
    this.xLabel?.remove();
    this.yLabel?.remove();
    this.pointerXLabel?.remove();
    this.pointerYLabel?.remove();
    this.layers.map((layer) => layer.remove());
    this.container?.exit().remove();
    this.outer?.exit().remove();
  }

  render() {
    // If this object has been initialized already, remove the existing
    // elements from the DOM before re-initializing and drawing.
    if (this.container) {
      this.remove();
    }

    this.initContainer();
    if (this.layers.length > 0) {
      this.draw();
    }
  }

  _idled() {
    this.idledTimeout = null;
  }

  setExtentByCoordinate(
    minCoordinate: number | undefined,
    maxCoordinate: number | undefined,
    animateDuration?: number | undefined
  ) {
    if (minCoordinate === undefined) {
      minCoordinate = this.minCoordinate();
    }
    if (maxCoordinate === undefined) {
      maxCoordinate = this.maxCoordinate();
    }
    if (animateDuration === undefined) {
      animateDuration = 100;
    }

    if (!this.xScale || !this.yScale) throw new Error("Uninitialized scales");
    const maxIntensity = Math.min(
      this.maxYDimBetween(minCoordinate, maxCoordinate) + 100.0,
      this.maxYDim()
    );

    this.extentCoordinateInterval = [minCoordinate, maxCoordinate];
    this.xScale.domain([minCoordinate, maxCoordinate]);
    this.yScale.domain([0, maxIntensity * 1.05]);
    this.xAxis
      ?.transition()
      .duration(animateDuration)
      .call(d3.axisBottom(this.xScale));
    this.yAxis
      ?.transition()
      .duration(animateDuration)
      .call(d3.axisLeft(this.yScale));
    if (this.brush != null) {
      const brush = this.brush;
      this.layers.map((layer) => layer.onBrush(brush));
    }
    this.layers.map((layer) => layer.redraw(this));
    this.emitUpdateEvent()
  }

  emitUpdateEvent() {
    this.emitEvent()
  }

  _emitUpdateEvent() {
    this.eventHandlers.forEach(f => {
      f(this)
    })
  }

  addRedrawEventListener(f: Function) {
    this.eventHandlers.push(f)
  }

  removeRedrawEventHandlers() {
    this.eventHandlers = []
  }

  updateChart() {
    let extent = d3.event.selection;
    if (!extent) {
      if (!this.idledTimeout) {
        this.idledTimeout = setTimeout(() => this._idled(), 350);
        return this.idledTimeout;
      }
      this.setExtentByCoordinate(this.minCoordinate(), this.maxCoordinate());
    } else {
      if (!this.xScale || !this.yScale) throw new Error("Uninitialized scales");
      const minCoordinate = this.xScale.invert(extent[0]);
      const maxCoordinate = this.xScale.invert(extent[1]);
      this.setExtentByCoordinate(minCoordinate, maxCoordinate);
    }
  }

  draw() {
    this.colorCycle.reset();
    this.layers.map((layer) => layer.initArtist(this));
    console.log(this.minCoordinate(), this.maxCoordinate())
  }
}

export class SpectrumCanvas extends MSCanvasBase<MZPoint> {}
