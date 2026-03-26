import { MZPeakReader, Spectrum } from "mzpeakts";
import {
  LayerBase,
  ProfileLayer,
  PrecursorPeakLayer,
  PointLike,
  ChargedPoint,
  MZPoint,
  PointLayer,
} from "./canvas/layers";
import { createContext, Dispatch, useContext, useReducer } from "react";

type SpectrumGroup = any

export class SpectrumData {
  spectrum: Spectrum;
  layers: (LayerBase<PointLike>)[];
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

  buildLayersSpectrum() {
    const spectrum = this.spectrum as Spectrum
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
            {}
          )
        );
      }
    } else {
      const points = spectrum.centroidPeaks() || [];
      if (points && points.length > 0) {
        this.layers.push(
          new PointLayer(
            points.map((p) => new MZPoint(p.mz, p.intensity)),
            {}
          )
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
            precIon.chargeState || 0
          );
          this.layers.push(new PrecursorPeakLayer(precursorPoint, {}));
        }
      }
    }
  }

  buildLayers() {
    return this.buildLayersSpectrum()
  }
}


type ProcessingParams = any;

export class SpectrumViewerState {
  spectrumData: SpectrumData | null;
  processingParams: ProcessingParams | null;
  mzReader: MZPeakReader<any> | null;
  currentSpectrumIdx: number | null;

  constructor(
    spectrumData: SpectrumData | null,
    processingParams: ProcessingParams,
    mzReader: MZPeakReader<any> | null,
    currentSpectrumIdx: number | null,
  ) {
    this.spectrumData = spectrumData;
    this.processingParams = processingParams;
    this.mzReader = mzReader;
    this.currentSpectrumIdx = currentSpectrumIdx;
  }

  copy() {
    return new SpectrumViewerState(
      this.spectrumData,
      this.processingParams,
      this.mzReader,
      this.currentSpectrumIdx,
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
      return spectrum
    }
  }

  static default() {
    return new this(null, null, null, null);
  }

  get spectraAvailable() {
    return this.mzReader ? this.mzReader.numSpectra : 0;
  }

  get currentSpectrumID() {
    return this.spectrumData?.id;
  }
}

export enum ViewerActionType {
  MZReader,
  CurrentSpectrumIdx,
  ProcessingParams,
}

export type SpectrumViewerAction =
  | { type: ViewerActionType.MZReader; value: MZPeakReader<any> | null }
  | { type: ViewerActionType.ProcessingParams; value: ProcessingParams | null }
  | { type: ViewerActionType.CurrentSpectrumIdx; value: number | null, spectrum?: Spectrum };

export const viewReducer = (
  state: SpectrumViewerState,
  action: SpectrumViewerAction
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
        nextState.spectrumData = new SpectrumData(action.spectrum)
      } else {
        nextState.spectrumData = null
      }
      break;
    }
    case ViewerActionType.ProcessingParams: {
      if (action.value != null) {
        nextState.processingParams = action.value;
      }
      break;
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
    SpectrumViewerState.default()
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
      ).toString(16)
  );
}
