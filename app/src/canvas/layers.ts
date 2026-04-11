import * as d3 from "d3";

import {
  SpectrumCanvas,
  MSCanvasBase,
} from "./canvas";
// import * as mzdata from "mzdata";
import * as mzpeakts from 'mzpeakts';

const defaultColor = "steelblue";

const dropZeroRuns = (x: number[]) => {
  const mask = [];
  let runStart = null;
  let runEnd = null;
  for (let i = 0; i < x.length; i++) {
    if (x[i] == 0) {
      if (runStart == null) {
        runStart = i;
      } else {
        runEnd = i;
      }
    } else {
      if (runStart == null) {
        mask.push(i);
      } else if (runEnd != null && runStart != null) {
        mask.push(runStart);
        mask.push(runEnd);
        runStart = null;
        runEnd = null;
      } else {
        mask.push(i);
        runStart = null;
      }
    }
  }
  if (runStart != null) {
    mask.push(runStart);
  }
  return mask;
};

const subsampleResolutionSpacing = (
  x: NumericArray,
  desiredResolution: number
) => {
  const keptIndices = [0];
  if (x.length == 0) return keptIndices;

  let last = x[0];
  for (let i = 1; i < x.length; i++) {
    if (x[i] - last > desiredResolution) {
      keptIndices.push(i);
      last = x[i];
    }
  }
  if (keptIndices[keptIndices.length - 1] != x.length - 1) {
    keptIndices.push(x.length - 1);
  }
  return keptIndices;
};

const arrayMask = (x: NumericArray, ii: number[]) => ii.map((i) => x[i]);

const neutralMass = (mz: number, charge: number) => {
  return mz * Math.abs(charge) - charge * 1.007;
};

const pointNeutralMass = (point: ChargedPoint) => {
  return neutralMass(point.mz, point.charge);
};

export interface PointLike {
  get x(): number;
  get y(): number;
  set x(x: number);
  set y(y: number);
  asPoint(): PointLike;
}

export type PointSelectionType<T extends PointLike> = d3.Selection<
  SVGGElement,
  T,
  HTMLElement,
  any
>;
export type PointListSelectionType<T extends PointLike> = d3.Selection<
  SVGPathElement,
  T[],
  HTMLElement,
  any
>;

export class MZPoint implements PointLike {
  mz: number;
  intensity: number;

  constructor(mz: number, intensity: number) {
    this.mz = mz;
    this.intensity = intensity;
  }

  static empty() {
    return new MZPoint(0, 0);
  }

  get x() {
    return this.mz;
  }

  get y() {
    return this.intensity;
  }

  set x(x: number) {
    this.mz = x;
  }

  set y(y: number) {
    this.intensity = y;
  }

  asPoint(): PointLike {
    return new MZPoint(this.mz, this.intensity);
  }
}

export class ChargedPoint extends MZPoint implements PointLike {
  charge: number;

  constructor(mz: number, intensity: number, charge: number) {
    super(mz, intensity);
    this.charge = charge;
  }
}

export class LabeledPoint extends ChargedPoint implements PointLike {
  label: string;

  constructor(mz: number, intensity: number, charge: number, label: string) {
    super(mz, intensity, charge);
    this.label = label;
  }
}

export class TimePoint implements PointLike {
  time: number;
  intensity: number;

  constructor(time: number, intensity: number) {
    this.time = time;
    this.intensity = intensity;
  }

  static empty() {
    return new MZPoint(0, 0);
  }

  get x() {
    return this.time;
  }

  get y() {
    return this.intensity;
  }

  set x(x: number) {
    this.time = x;
  }

  set y(y: number) {
    this.intensity = y;
  }

  asPoint(): PointLike {
    return new TimePoint(this.time, this.intensity);
  }
}

function pointToProfile<T extends PointLike>(points: T[]): T[] {
  const result = [];
  for (const point of points) {
    const beforePoint = point.asPoint();
    const afterPoint = point.asPoint();
    beforePoint.x -= 1e-6;
    beforePoint.y = -1;
    result.push(beforePoint);
    result.push(point);
    afterPoint.x += 1e-6;
    afterPoint.y = -1;
    result.push(afterPoint);
  }
  return result as T[];
}

export abstract class LayerBase<T extends PointLike> {
  abstract get length(): number;
  abstract get(i: number): PointLike;
  abstract initArtist(canvas: MSCanvasBase<T>): void;
  abstract onBrush(brush: d3.BrushBehavior<unknown>): void;
  abstract remove(): void;
  abstract redraw(canvas: MSCanvasBase<T>): void;
  abstract onHover(canvas: MSCanvasBase<T>, value: any): void;

  asArray(): PointLike[] {
    return Array.from(this);
  }

  [Symbol.iterator](): Iterator<PointLike> {
    let self = this;
    let i = 0;
    const iterator = {
      next() {
        if (i >= self.length) {
          return { value: self.get(0), done: true };
        }
        const value = self.get(i);
        i++;
        return { value: value, done: false };
      },
    };
    return iterator;
  }

  maxX() {
    if (this.length === 0) {
      return 0;
    }
    const point = this.get(this.length - 1);
    return point.x;
  }

  minX() {
    if (this.length === 0) {
      return 0;
    }
    const point = this.get(0);
    return point.x;
  }

  minCoordinate() {
    return this.minX();
  }

  maxCoordinate() {
    return this.maxX();
  }

  maxY() {
    let maxValue = 0;
    for (let point of this) {
      if (!point) continue;
      if (point.y > maxValue) {
        maxValue = point.y;
      }
    }
    return maxValue;
  }

  minY() {
    return 0;
  }

  searchX(mz: number) {
    if (mz > this.maxX()) {
      return this.length - 1;
    } else if (mz < this.minX()) {
      return 0;
    }
    let lo = 0;
    let hi = this.length - 1;

    while (hi !== lo) {
      let mid = Math.trunc((hi + lo) / 2);
      let value = this.get(mid).x;
      let diff = value - mz;
      if (Math.abs(diff) < 1e-3) {
        let bestIndex = mid;
        let bestError = Math.abs(diff);
        let i = mid;
        while (i > -1) {
          value = this.get(i).x;
          diff = Math.abs(value - mz);
          if (diff < bestError) {
            bestIndex = i;
            bestError = diff;
          } else {
            break;
          }
          i--;
        }
        i = mid + 1;
        while (i < this.length) {
          value = this.get(i).x;
          diff = Math.abs(value - mz);
          if (diff < bestError) {
            bestIndex = i;
            bestError = diff;
          } else {
            break;
          }
          i++;
        }
        return bestIndex;
      } else if (hi - lo === 1) {
        let bestIndex = mid;
        let bestError = Math.abs(diff);
        let i = mid;
        while (i > -1) {
          value = this.get(i).x;
          diff = Math.abs(value - mz);
          if (diff < bestError) {
            bestIndex = i;
            bestError = diff;
          } else {
            break;
          }
          i--;
        }
        i = mid + 1;
        while (i < this.length) {
          value = this.get(i).x;
          diff = Math.abs(value - mz);
          if (diff < bestError) {
            bestIndex = i;
            bestError = diff;
          } else {
            break;
          }
          i++;
        }
        return bestIndex;
      } else if (diff > 0) {
        hi = mid;
      } else {
        lo = mid;
      }
    }
    return 0;
  }

  matchX(mz: number, errorTolerance: number) {
    let i = this.searchX(mz);
    let pt = this.get(i);
    if (Math.abs(pt.x - mz) / mz < errorTolerance) {
      return pt;
    }
    return null;
  }

  abstract slice(begin: number, end: number): LayerBase<T>;

  between(beginX: number, endX: number) {
    let startIdx = this.searchX(beginX);
    while (startIdx > 0 && this.get(startIdx).x > beginX) {
      startIdx--;
    }
    if (this.get(startIdx).x < beginX) startIdx++;

    let endIdx = startIdx;
    while (endIdx < this.length && this.get(endIdx).x < endX) {
      endIdx++;
    }
    return this.slice(startIdx, endIdx);
  }

  //   between(beginMz: number, endMz: number) {
  //     return this.slice(this.searchMz(beginMz), this.searchMz(endMz));
  //   }
}

export abstract class DataLayer<T extends PointLike> extends LayerBase<T> {
  metadata: any;
  _color: string | null;
  points: T[];
  line: PointSelectionType<PointLike> | null;
  path: PointListSelectionType<PointLike> | null;
  brushPatch: PointSelectionType<PointLike> | null;

  strokeWidth: number;

  constructor(metadata: any) {
    super();
    this.metadata = metadata;
    this._color = null;
    this.points = [];
    this.line = null;
    this.path = null;
    this.brushPatch = null;
    this.strokeWidth = metadata.strokeWidth ? metadata.strokeWidth : 1.5;
  }

  sortX() {
    return Array.from(this.points).sort((a, b) => {
      if (a.x < b.x) {
        return -1;
      } else if (a.x > b.x) {
        return 1;
      } else {
        return 0;
      }
    });
  }

  get color(): string {
    return this._color === null ? defaultColor : this._color;
  }

  set color(value: string | null) {
    this._color = value;
  }

  get layerType() {
    return "data";
  }

  onBrush(brush: any) {
    if (this.line) this.line.select(".brush").call(brush.move, null);
  }

  onHover(_canvas: MSCanvasBase<T>, _cursorInfo: any) {
    return;
  }

  redraw(canvas: MSCanvasBase<T>) {
    if (!this.line) return;
    const lineAttrs = this.buildPathCoords(canvas);
    this.line
      .select(".line")
      .transition("DataLayer")
      .attr("d", lineAttrs(this._makeData()) || "");
  }

  remove() {
    this.line?.remove();
    this.path?.remove();
  }

  buildPathCoords(canvas: MSCanvasBase<T>) {
    const path = d3
      .line<PointLike>()
      .x((d) => (canvas.xScale ? canvas.xScale(d.x) || 0 : 0))
      .y((d) => (canvas.yScale ? canvas.yScale(d.y) || 0 : 0));
    return path;
  }

  _makeData(): PointLike[] {
    return pointToProfile(this.asArray());
  }

  styleArtist(path: PointListSelectionType<PointLike>) {
    return path
      .attr("stroke", this.color)
      .attr("stroke-width", this.strokeWidth)
      .attr("fill", "none");
  }

  initArtist(canvas: MSCanvasBase<T>) {
    if (!canvas.container) throw new Error("Uninitialized canvas container");
    this.line = canvas.container.append("g").attr("clip-path", "url(#clip)");
    this.color = canvas.colorCycle.nextColor();
    const points = this._makeData();

    this.path = this.styleArtist(
      this.line
        .append("path")
        .datum(points)
        .attr("class", `line ${this.layerType}`)
    );

    const coords = this.buildPathCoords(canvas)(points) || "";
    this.path.attr("d", coords);

    if (canvas.brush) {
      this.brushPatch = this.line
        .append("g")
        .attr("class", "brush")
        .call(canvas.brush);
    }
  }
}

export class LineArtist<T extends PointLike> extends DataLayer<T> {
  label: string;
  strokeWidth: number;

  get length(): number {
    return this.points.length;
  }

  constructor(points: T[], metadata: any) {
    super(metadata);
    this.points = points;
    this.points = this.sortX();
    this.line = null;
    this.label = metadata.label ? metadata.label : "";
    this._color = metadata.color ? metadata.color : defaultColor;
    this.strokeWidth = metadata.strokeWidth ? metadata.strokeWidth : 2.5;
  }

  sortX() {
    return Array.from(this.points).sort((a, b) => {
      if (a.x < b.x) {
        return -1;
      } else if (a.x > b.x) {
        return 1;
      } else {
        return 0;
      }
    });
  }

  get(i: number) {
    return this.points[i];
  }

  slice(begin: number, end: number): LayerBase<T> {
    return new LineArtist(this.points.slice(begin, end), this.metadata);
  }

  _makeData() {
    const result = pointToProfile(this.points);
    return result;
  }

  styleArtist(path: PointListSelectionType<PointLike>) {
    return path
      .attr("stroke", this.color)
      .attr("stroke-width", this.strokeWidth)
      .attr("fill", "none");
  }

  initArtist(canvas: MSCanvasBase<T>) {
    if (!canvas.container) return;
    this.line = canvas.container.append("g").attr("clip-path", "url(#clip)");
    const points = this._makeData();

    const path = this.line
      .append("path")
      .datum(points as PointLike[])
      .attr("class", `line ${this.layerType}`);

    this.path = this.styleArtist(path);

    this.path.attr("d", this.buildPathCoords(canvas)(points) || "");
  }
}

type NumericArray = Float32Array | Float64Array | number[];

interface ProfileOptions {
  subsample?: boolean
}

export class ProfileLayer<T extends PointLike> extends DataLayer<T> {
  get length(): number {
    return this.x.length;
  }

  x: NumericArray;
  y: NumericArray;
  subsample: boolean;

  constructor(x: NumericArray, y: NumericArray, metadata: ProfileOptions) {
    super(metadata);
    this.subsample = false;
    if (x.length > 5e4) {
      this.subsample = true;
    }
    if (metadata.subsample != undefined) {
      this.subsample = Boolean(metadata.subsample);
    }
    this.x = x;
    this.y = y;
  }

  _makeData() {
    if (this.subsample) {
      const spacing = subsampleResolutionSpacing(this.x, 0.001);
      let subsampledX = arrayMask(this.x, spacing);
      let subsampledIntensity = arrayMask(this.y, spacing);
      const liveIndices = dropZeroRuns(subsampledIntensity);
      subsampledX = arrayMask(subsampledX, liveIndices);
      subsampledIntensity = arrayMask(subsampledIntensity, liveIndices);
      return subsampledX.map((x, i) => {
        return new MZPoint(x, subsampledIntensity[i]);
      });
    } else {
      return this.asArray();
    }
  }

  get(i: number) {
    return new MZPoint(this.x[i], this.y[i]);
  }

  get basePeak() {
    let bestIndex = 0;
    let bestValue = -1;
    for (let i = 0; i < this.length; i++) {
      let val = this.y[i];
      if (val > bestValue) {
        bestValue = val;
        bestIndex = i;
      }
    }
    return new MZPoint(this.x[bestIndex], this.y[bestIndex]);
  }

  slice(begin: number, end: number): LayerBase<MZPoint> {
    return new ProfileLayer(
      this.x.slice(begin, end),
      this.y.slice(begin, end),
      this.metadata,
    );
  }

  get layerType() {
    return "profile-layer";
  }
}

export class PointLayer<T extends PointLike> extends DataLayer<T> {
  label: PointSelectionType<PointLike> | null;

  get length() {
    return this.points.length;
  }

  constructor(points: T[], metadata: any) {
    super(metadata);
    this.points = points;
    this.points.sort((a, b) => {
      if (a.x < b.x) {
        return -1;
      } else if (a.x > b.x) {
        return 1;
      } else {
        return 0;
      }
    });
    this.line = null;
    this.label = null;
  }

  get basePeak() {
    return this.points.reduce((a, b) => (a.y > b.y ? a : b));
  }

  get(i: number) {
    return this.points[i];
  }

  get layerType() {
    return "centroid-layer";
  }

  slice(begin: number, end: number): PointLayer<T> {
    return new PointLayer(this.points.slice(begin, end), this.metadata);
  }

  _makeData() {
    const result = pointToProfile(this.points);
    return result;
  }

  onHover(canvas: MSCanvasBase<T>, cursorInfo: any) {
    if (!canvas.xScale || !canvas.yScale || !canvas.container) return;
    let mz = cursorInfo.mz;
    let index = this.searchX(mz);
    let peak = this.get(index);
    if (peak === undefined) {
      return;
    }
    if (Math.abs(peak.x - mz) > 0.3) {
      if (this.label !== null) {
        this.label.remove();
        this.label = null;
      }
      return;
    }
    let mzPosition = canvas.xScale(peak.x);
    let intensityPosition = canvas.yScale(peak.y) || 0;
    if (this.label !== null) {
      this.label.remove();
    }
    this.label = canvas.container
      .append("g")
      .attr("transform", `translate(${mzPosition},${intensityPosition - 10})`);
    this.label
      .append("text")
      .text(peak.x.toFixed(3))
      .style("text-anchor", "middle")
      .attr("class", "peak-label");
  }

  remove() {
    super.remove();
    if (this.label !== null) {
      this.label.remove();
    }
  }
}

export class NeutralMassPointLayer extends PointLayer<ChargedPoint> {
  points: ChargedPoint[];
  pointsByMass: ChargedPoint[];

  constructor(points: any[], metadata: any) {
    super(points, metadata);
    if (!(points[0] instanceof ChargedPoint)) {
      points = points.map((p) => new ChargedPoint(p.mz, p.intensity, p.charge));
    }
    this.points = points;
    this.pointsByMass = this.sortMass();
  }

  get(i: number): ChargedPoint {
    return this.points[i];
  }

  sortMass() {
    return Array.from(this.points).sort((a, b) => {
      if (pointNeutralMass(a) < pointNeutralMass(b)) {
        return -1;
      } else if (pointNeutralMass(a) > pointNeutralMass(b)) {
        return 1;
      } else {
        return 0;
      }
    });
  }

  maxMass() {
    return pointNeutralMass(this.pointsByMass[this.pointsByMass.length - 1]);
  }

  minMass() {
    return pointNeutralMass(this.pointsByMass[0]);
  }

  getOverMass(i: number): ChargedPoint {
    return this.pointsByMass[i];
  }

  searchMass(mass: number) {
    if (mass > this.maxMass()) {
      return this.length - 1;
    } else if (mass < this.minMass()) {
      return 0;
    }
    let lo = 0;
    let hi = this.length - 1;

    while (hi !== lo) {
      let mid = Math.trunc((hi + lo) / 2);
      let value = pointNeutralMass(this.getOverMass(mid));
      let diff = value - mass;
      if (Math.abs(diff) < 1e-3) {
        let bestIndex = mid;
        let bestError = Math.abs(diff);
        let i = mid;
        while (i > -1) {
          value = pointNeutralMass(this.getOverMass(i));
          diff = Math.abs(value - mass);
          if (diff < bestError) {
            bestIndex = i;
            bestError = diff;
          } else {
            break;
          }
          i--;
        }
        i = mid + 1;
        while (i < this.length) {
          value = pointNeutralMass(this.getOverMass(i));
          diff = Math.abs(value - mass);
          if (diff < bestError) {
            bestIndex = i;
            bestError = diff;
          } else {
            break;
          }
          i++;
        }
        return bestIndex;
      } else if (hi - lo === 1) {
        let bestIndex = mid;
        let bestError = Math.abs(diff);
        let i = mid;
        while (i > -1) {
          value = pointNeutralMass(this.getOverMass(i));
          diff = Math.abs(value - mass);
          if (diff < bestError) {
            bestIndex = i;
            bestError = diff;
          } else {
            break;
          }
          i--;
        }
        i = mid + 1;
        while (i < this.length) {
          value = pointNeutralMass(this.getOverMass(i));
          diff = Math.abs(value - mass);
          if (diff < bestError) {
            bestIndex = i;
            bestError = diff;
          } else {
            break;
          }
          i++;
        }
        return bestIndex;
      } else if (diff > 0) {
        hi = mid;
      } else {
        lo = mid;
      }
    }
    return 0;
  }

  matchMass(mass: number, errorTolerance: number) {
    let i = this.searchMass(mass);
    let pt = this.getOverMass(i);
    if (Math.abs(pointNeutralMass(pt) - mass) / mass < errorTolerance) {
      return pt;
    }
    return null;
  }
}

export class LabeledPeakLayer extends NeutralMassPointLayer {
  seriesLabel: string;
  points: LabeledPoint[];
  labels: d3.Selection<
    SVGTextElement,
    LabeledPoint,
    SVGGElement,
    PointLike
  > | null;

  constructor(points: LabeledPoint[], metadata: any) {
    super(points, metadata);
    this.points = points;
    this._color = this.metadata.color;
    this.seriesLabel =
      this.metadata.seriesLabel ||
      "labeled-peaks-" + Math.floor(Math.random() * 1e16);
    this.labels = null;
  }

  initArtist(canvas: MSCanvasBase<PointLike>) {
    if (!canvas.container) return;
    const canvasAs = canvas as any as MSCanvasBase<ChargedPoint>;
    super.initArtist(canvasAs);
    this._drawLabels(canvasAs);
  }

  _drawLabels(canvas: SpectrumCanvas) {
    if (!canvas.container || !canvas.xScale || !canvas.yScale) return;
    if (this.labels) {
      this.labels.remove();
    }
    this.labels = canvas.container
      .selectAll(`text.peak-label.${this.seriesLabel}`)
      .data(this.points)
      .enter()
      .append("g")
      .attr("class", `label-${this.seriesLabel}`)
      .attr(
        "transform",
        (d) =>
          `translate(${canvas.xScale ? canvas.xScale(d.mz) : 0},${
            canvas.yScale ? canvas.yScale(d.intensity) || 0 - 10 : 0
          })`
      )
      .append("text")
      .text((d) => d.label)
      .attr("text-anchor", "middle");
  }

  redraw(canvas: MSCanvasBase<PointLike>) {
    super.redraw(canvas as MSCanvasBase<ChargedPoint>);
    this._drawLabels(canvas as MSCanvasBase<ChargedPoint>);
  }

  remove() {
    super.remove();
    if (this.labels) {
      this.labels.remove();
    }
  }
}


class AbstractPointLayer<T extends PointLike> extends PointLayer<T> {
  slice(_begin: number, _end: number): AbstractPointLayer<T> {
    return new AbstractPointLayer([], {});
  }
}

export class PrecursorPeakLayer extends AbstractPointLayer<ChargedPoint> {
  mz: number;
  intensity: number;
  charge: number;
  precursorLabel: d3.Selection<
    SVGTextElement,
    PointLike,
    HTMLElement,
    any
  > | null;

  constructor(peak: ChargedPoint, metadata: any) {
    super([peak], metadata);
    this.mz = peak.mz;
    this.intensity = peak.intensity;
    this.charge = peak.charge;
    this.precursorLabel = null;
  }

  maxY() {
    return 1;
  }

  get layerType() {
    return "precursor-layer";
  }

  addLabel(canvas: MSCanvasBase<PointLike>) {
    const canvasAs = canvas as MSCanvasBase<ChargedPoint>;
    if (!canvasAs.container) return;
    const lines = [
      `Prec. m/z: ${this.mz.toFixed(3)}`,
      `Prec. z: ${this.charge}`,
      `Prec. mass: ${neutralMass(this.mz, this.charge).toFixed(3)}`,
    ];

    this.precursorLabel = canvasAs.container
      .append("text")
      .attr(
        "transform",
        `translate(${canvas.width * 0.85},${canvas.height * 0.02})`
      )
      .style("text-anchor", "left")
      .attr("class", "precursor-label");
    this.precursorLabel
      .selectAll("tspan.precursor-label-row")
      .data(lines)
      .enter()
      .append("tspan")
      .attr("dx", 10)
      .attr("dy", 16)
      .attr("x", 0)
      .text((d) => d);
  }

  initArtist(canvas: MSCanvasBase<PointLike>) {
    super.initArtist(canvas as MSCanvasBase<ChargedPoint>);
    this.addLabel(canvas);
  }

  styleArtist(path: PointListSelectionType<PointLike>) {
    let gapSize = 10;
    let dashSize = 5;
    return super
      .styleArtist(path)
      .attr("stroke-dasharray", `${dashSize} 1 ${gapSize}`);
  }

  remove() {
    super.remove();
    if (this.precursorLabel) {
      this.precursorLabel.remove();
    }
  }
}

export class IsolationWindowLayer extends AbstractPointLayer<MZPoint> {
  windows: mzpeakts.IsolationWindow[];
  height: number;

  constructor(
    windows: mzpeakts.IsolationWindow[],
    height: number,
    metadata: any
  ) {
    super(IsolationWindowLayer._splitWindows(windows, height), metadata);
    this.windows = windows;
    this.height = height;
  }

  maxY() {
    return 1;
  }

  get layerType() {
    return "isolation-window-layer";
  }

  onHover(_canvas: SpectrumCanvas, _cursorInfo: any) {
    return;
  }

  static _splitWindows(windows: mzpeakts.IsolationWindow[], height: number) {
    let points = [];
    for (let window of windows) {
      points.push(new MZPoint(window.lowerBound, height));
      points.push(new MZPoint(window.upperBound, height));
    }
    return points;
  }

  styleArtist(path: any) {
    let gapSize = 5;
    let dashSize = 5;
    return super
      .styleArtist(path)
      .attr("stroke-dasharray", `${dashSize} ${gapSize}`);
  }
}

export interface Point3D {
  mz: number;
  time: number;
  intensity: number;
}
