import { MzPeakReader, Spectrum, Chromatogram, XIC } from "mzpeakts";

import {
  LayerBase,
  ProfileLayer,
  PrecursorPeakLayer,
  PointLike,
  ChargedPoint,
  MZPoint,
  PointLayer,
} from "./canvas/layers";

import {
  createContext,
  Dispatch,
  ReactNode,
  useContext,
  useReducer,
} from "react";

export type StatusMessage = {
  text: string | null;
  icon: ReactNode | null;
};

type SpectrumGroup = any;

export class SpectrumData {
  spectrum: Spectrum;
  layers: LayerBase<PointLike>[];
  group: SpectrumGroup | undefined;

  get id() {
    return this.spectrum.id;
  }

  get scanRange() {
    const event = this.spectrum.scans[0];
    if (event) {
      const scanWindow = event.scanWindows[0];
      return {
        lowerBound: scanWindow.lowerBound,
        upperBound: scanWindow.upperBound,
      };
    }
  }

  constructor(spectrum: Spectrum, group?: SpectrumGroup) {
    this.spectrum = spectrum;
    this.group = group;
    this.layers = [];
    this.buildLayers();
  }

  buildLayers() {
    const spectrum = this.spectrum as Spectrum;
    if (spectrum.isProfile) {
      const arrayTable = spectrum.dataArrays;
      if (!arrayTable) return;
      const mzs = arrayTable["m/z array"] as Float64Array;
      const intensities = arrayTable["intensity array"] as Float32Array;
      this.layers.push(new ProfileLayer(mzs, intensities, {}));
      const centroids = spectrum.centroidPeaks();
      if (centroids && centroids.length > 0) {
        this.layers.push(
          new PointLayer(
            centroids.map((p) => new MZPoint(p.mz, p.intensity)),
            {},
          ),
        );
      }
    } else {
      const points = spectrum.centroidPeaks() || [];
      if (points && points.length > 0) {
        this.layers.push(
          new PointLayer(
            points.map((p) => new MZPoint(p.mz, p.intensity)),
            {},
          ),
        );
      }
    }
    if (spectrum.msLevel > 1) {
      if (spectrum.selectedIons) {
        const precIon = spectrum.selectedIons[0];
        if (precIon.mz) {
          const precursorPoint = new ChargedPoint(
            precIon.mz,
            precIon.intensity || 0,
            precIon.chargeState || 0,
          );
          this.layers.push(new PrecursorPeakLayer(precursorPoint, {}));
        }
      }
    }
  }
}

export class ChromatogramData {
  chromatogram: Chromatogram | null;
  rawExtraction: XIC | null
  layers: LayerBase<PointLike>[];

  constructor(chromatogram: Chromatogram | null, rawExtraction: XIC | null) {
    this.chromatogram = chromatogram
    this.rawExtraction = rawExtraction
    this.layers = []
    this.buildLayers()
  }

  get id() {
    if (this.rawExtraction) {
      const t = this.rawExtraction.target
      const parts = []
      if (t.timeRange) {
        parts.push(`time_${t.timeRange.start}_${t.timeRange.end}`);
      }
      if (t.mzRange) {
        parts.push(`mz_${t.mzRange.start}_${t.mzRange.end}`)
      }
      return parts.join("_")
    }
    if (this.chromatogram) {
      return this.chromatogram.id
    }
    return "nil"
  }

  buildChromatogramLayers() {
    if (!this.chromatogram) return
    if (!this.chromatogram.dataArrays) return

    this.layers.push(
      new ProfileLayer(
        this.chromatogram.dataArrays["time array"] as Float64Array,
        this.chromatogram.dataArrays["intensity array"] as Float32Array,
        { subsample: false },
      ),
    );
  }

  buildXIC() {
    if (!this.rawExtraction) return
    const times = new Float64Array(this.rawExtraction.points.length);
    const intensities = new Float32Array(this.rawExtraction.points.length);
    for(let i = 0; i < this.rawExtraction.points.length; i++) {
      const e = this.rawExtraction.points[i];
      const total = (e.dataArrays["intensity array"] as Float32Array).reduce((a, b) => a + b, 0);
      intensities[i] = total
      times[i] = e.time != null ? e.time : Number(e.index)
    }
    this.layers.push(new ProfileLayer(times, intensities, {subsample: false}))
  }

  buildLayers() {
    if (this.rawExtraction)
      this.buildXIC()
    if (this.chromatogram)
      this.buildChromatogramLayers()

  }
}

type ProcessingParams = any;

export class SpectrumViewerState {
  spectrumData: SpectrumData | null;
  chromatogramData: ChromatogramData | null;
  processingParams: ProcessingParams | null;
  mzReader: MzPeakReader<any> | null;
  currentSpectrumIdx: number | null;
  statusMessage: StatusMessage;

  constructor(
    spectrumData: SpectrumData | null,
    processingParams: ProcessingParams,
    mzReader: MzPeakReader<any> | null,
    currentSpectrumIdx: number | null,
    statusMessage: StatusMessage = { text: null, icon: null },
    chromatogramData: ChromatogramData | null = null,
  ) {
    this.spectrumData = spectrumData;
    this.processingParams = processingParams;
    this.mzReader = mzReader;
    this.currentSpectrumIdx = currentSpectrumIdx;
    this.statusMessage = statusMessage;
    this.chromatogramData = chromatogramData
  }

  copy() {
    return new SpectrumViewerState(
      this.spectrumData,
      this.processingParams,
      this.mzReader,
      this.currentSpectrumIdx,
      this.statusMessage,
      this.chromatogramData
    );
  }

  loadCurrentGroup(spectrum: Spectrum) {
    if (
      (spectrum.msLevel || 0) == 1 &&
      this.mzReader &&
      this.currentSpectrumIdx
    ) {
      // this.mzReader.setDataLoading(true);
      // const group = this.mzReader.groupAt(this.currentSpectrumIdx);
      // this.mzReader.setDataLoading(false);
      // return group;
      return spectrum;
    }
  }

  async extractXIC(
    timeStart: number,
    timeEnd: number,
    mzStart: number,
    mzEnd: number,
  ) {
    if (!this.mzReader) {
      return null;
    }
    if (!this.mzReader.spectrumMetadata) {
      return null;
    }
    const xic = await this.mzReader.extractXIC(
      { start: timeStart, end: timeEnd },
      { start: mzStart, end: mzEnd },
    );
    return xic
  }

  static default() {
    return new this(null, null, null, null);
  }

  get chromatogramsAvailable() {
    return this.mzReader ? this.mzReader.numChromatograms : 0;
  }

  get spectraAvailable() {
    return this.mzReader ? this.mzReader.numSpectra : 0;
  }

  get currentChromatogramID() {
    return this.chromatogramData?.id
  }

  get currentSpectrumID() {
    return this.spectrumData?.id;
  }
}

export enum ViewerActionType {
  MZReader,
  CurrentSpectrumIdx,
  CurrentChromatogramIdx,
  ProcessingParams,
  StatusMessage,
  XICExtract,
}

export type SpectrumViewerAction =
  | { type: ViewerActionType.MZReader; value: MzPeakReader<any> | null }
  | { type: ViewerActionType.ProcessingParams; value: ProcessingParams | null }
  | {
      type: ViewerActionType.CurrentSpectrumIdx;
      value: number | null;
      spectrum?: Spectrum;
    }
  | {
      type: ViewerActionType.CurrentChromatogramIdx,
      value: number | null;
      chromatogram?: Chromatogram
    }
  | {
      type: ViewerActionType.StatusMessage;
      text?: string | null;
      icon?: ReactNode | null;
    }
  | {
    type: ViewerActionType.XICExtract,
    target: XIC
  };

export const viewReducer = (
  state: SpectrumViewerState,
  action: SpectrumViewerAction,
) => {
  const nextState = state.copy();
  switch (action.type) {
    case ViewerActionType.MZReader: {
      nextState.mzReader = action.value;
      nextState.currentSpectrumIdx = null;
      nextState.spectrumData = null;
      break;
    }
    case ViewerActionType.CurrentSpectrumIdx: {
      nextState.currentSpectrumIdx = action.value;
      if (action.spectrum) {
        nextState.spectrumData = new SpectrumData(action.spectrum);
      } else {
        nextState.spectrumData = null;
      }
      break;
    }
    case ViewerActionType.CurrentChromatogramIdx: {
      nextState.currentSpectrumIdx = action.value;
      if (action.chromatogram) {
        nextState.chromatogramData = new ChromatogramData(action.chromatogram, null);
      } else {
        nextState.chromatogramData = null;
      }
      break;
    }
    case ViewerActionType.ProcessingParams: {
      if (action.value != null) {
        nextState.processingParams = action.value;
      }
      break;
    }
    case ViewerActionType.StatusMessage: {
      nextState.statusMessage = {
        text:
          action.text !== undefined ? action.text : state.statusMessage.text,
        icon:
          action.icon !== undefined ? action.icon : state.statusMessage.icon,
      };
      break;
    }
    case ViewerActionType.XICExtract: {
      nextState.chromatogramData = new ChromatogramData(null, action.target);
    }
  }
  return nextState;
};

const SpectrumViewerContext = createContext(SpectrumViewerState.default());
const SpectrumViewerDispatchContext =
  createContext<Dispatch<SpectrumViewerAction> | null>(null);

interface SpectrumProviderProps {
  children: (string | JSX.Element)[] | (string | JSX.Element);
}

export function SpectrumViewerProvider({ children }: SpectrumProviderProps) {
  const [state, dispatch] = useReducer(
    viewReducer,
    SpectrumViewerState.default(),
  );
  return (
    <SpectrumViewerContext.Provider value={state}>
      <SpectrumViewerDispatchContext.Provider value={dispatch}>
        {children}
      </SpectrumViewerDispatchContext.Provider>
    </SpectrumViewerContext.Provider>
  );
}

export function useSpectrumViewer() {
  return useContext(SpectrumViewerContext);
}

export function useSpectrumViewerDispatch(): Dispatch<SpectrumViewerAction> {
  const ctx = useContext(SpectrumViewerDispatchContext);
  if (ctx == null) {
    throw new Error("Using SpectrumViewerState out of context!");
  }
  return ctx;
}

export //https://stackoverflow.com/a/2117523/1137920
function uuidv4(): string {
  return (([1e7] as any) + -1e3 + -4e3 + -8e3 + -1e11).replace(
    /[018]/g,
    (c: number) =>
      (
        c ^
        (crypto.getRandomValues(new Uint8Array(1))[0] & (15 >> (c / 4)))
      ).toString(16),
  );
}
