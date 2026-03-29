import * as React from "react";
import { Spectrum } from "mzpeakts";
import {
  ScanRange,
  SpectrumCanvas,
} from "./canvas";
import "./component.css";
import {
  LayerBase,
  MZPoint,
  PointLike,

} from "./layers";
import { useSpectrumViewer, uuidv4 } from "../util";
import useMediaQuery from "@mui/material/useMediaQuery";

export interface SpectrumData {
  id: string;
  spectrum: Spectrum;
  layers: LayerBase<PointLike>[];
  scanRange?: ScanRange;
}

export interface CanvasProps {
  spectrumData: SpectrumData | null;
}

export enum CanvasActionType {
  SetData,
  CreateCanvas,
  ToggleFeatureProfiles,
  RenderCanvas,
  ResizeCanvas,
}

function getWindowDimensions() {
  const { innerWidth: width, innerHeight: height } = window;
  return {
    width,
    height,
  };
}

export default function useWindowDimensions() {
  const [windowDimensions, setWindowDimensions] = React.useState(
    getWindowDimensions(),
  );

  React.useEffect(() => {
    function handleResize() {
      setWindowDimensions(getWindowDimensions());
    }

    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  return windowDimensions;
}


export type SpectrumCanvasAction =
  | { type: CanvasActionType.SetData; data: SpectrumData | null }
  | {
      type: CanvasActionType.CreateCanvas;
      canvas: SpectrumCanvas | null;
    }
  | {
      type: CanvasActionType.ToggleFeatureProfiles;
    }
  | {
      type: CanvasActionType.RenderCanvas;
    }
  | {
    type: CanvasActionType.ResizeCanvas;
    width: number;
    height: number;
  };

export class CanvasState {
  id: string;
  spectrumData: SpectrumData | null;
  canvas: SpectrumCanvas | null;
  showFeatureProfiles: boolean;
  canvasHolder: React.MutableRefObject<HTMLDivElement | null>;
  windowSize: { width: number; height: number } | null;

  constructor(
    id: string,
    spectrumData: SpectrumData | null,
    canvas: SpectrumCanvas | null,
    showFeatureProfiles: boolean,
    canvasHolder: React.MutableRefObject<HTMLDivElement | null>,
    windowSize?: { width: number; height: number },
  ) {
    if (!windowSize) {
      windowSize = getWindowDimensions();
    }
    this.id = id;
    this.spectrumData = spectrumData;
    this.canvas = canvas;
    this.showFeatureProfiles = showFeatureProfiles;
    this.canvasHolder = canvasHolder;
    this.windowSize = windowSize
  }

  copy() {
    return new CanvasState(
      this.id,
      this.spectrumData,
      this.canvas,
      this.showFeatureProfiles,
      this.canvasHolder,
    );
  }

  static createEmpty(
    canvasHolder: React.MutableRefObject<HTMLDivElement | null>,
  ) {
    return new CanvasState(uuidv4(), null, null, false, canvasHolder);
  }

  isSpectrum() {
    return this.spectrumData?.spectrum instanceof Spectrum;
  }

  isCanvasCompatibleWithData() {
    const yesSpec = this.canvas instanceof SpectrumCanvas && this.isSpectrum();
    return yesSpec;
  }

  createCanvas() {
    const isSpectrum = this.isSpectrum();
    if (this.canvasHolder.current) {
      if (this.canvas != null) {
        this.clearCanvas();
      }
      if (isSpectrum) {
        let height = (this.windowSize?.height ?? 0) * 0.5;
        const width = (this.windowSize?.width ?? 0) * 0.6;
        if (width && height && height > (width * 1.5))  {
          height = width
        }
        console.log("Window size", width, height)
        this.canvas = new SpectrumCanvas(
          `#${this.canvasHolder.current.id}`,
          width,
          height,
          undefined,
          [],
          this.spectrumData?.id,
          this.spectrumData?.scanRange,
        );
      } else {
        this.clearCanvas();
        this.canvas = null;
      }
    }
  }

  clearCanvas() {
    this.canvas?.clear();
    this.canvas?.removeRedrawEventHandlers();

    // Since the operation
    if (this.canvasHolder.current) {
      while (this.canvasHolder.current.firstChild) {
        this.canvasHolder.current.removeChild(
          this.canvasHolder.current.firstChild,
        );
      }
    }
  }

  renderCanvas() {
    this.clearCanvas();
    if (this.canvasHolder.current == null) return;
    if (this.spectrumData == null) {
      this.clearCanvas();
      return;
    }
    if (this.canvas == null) {
      this.createCanvas();
      if (this.canvas == null) {
        return;
      }
    }
    const idMatch = this.canvas?.spectrumID == this.spectrumData?.id;
    console.log(
      `${idMatch}: ${this.canvas?.spectrumID} ${this.spectrumData?.id}`,
    );
    if (this.canvas?.layers !== this.spectrumData?.layers) {
      let extent = this.canvas.extentCoordinateInterval;

      this.clearCanvas();
      this.canvas.spectrumID = this.spectrumData.id;

      if (!idMatch) {
        this.canvas.setExtentByCoordinate(undefined, undefined);
      } else if (extent !== undefined) {
        if (!(extent[0] === 0 && extent[1] === 0)) {
          this.canvas.setExtentByCoordinate(extent[0], extent[1]);
        }
      }
      this.canvas.addLayers(this.spectrumData.layers as LayerBase<MZPoint>[]);
      this.canvas.render();
    } else if (!idMatch) {
      this.clearCanvas();
      this.canvas.spectrumID = this.spectrumData.id;
      this.canvas.setExtentByCoordinate(undefined, undefined);
      this.canvas.addLayers(this.spectrumData.layers as LayerBase<MZPoint>[]);
      this.canvas.render();
    }
  }

  checkBeforeRender() {
    if (this.canvas === null) {
      if (this.spectrumData) {
        this.createCanvas();
        return true;
      } else {
        return false;
      }
    }
    if (this.spectrumData == null) {
      this.clearCanvas();
      return false;
    }
    if (!this.isCanvasCompatibleWithData()) {
      this.clearCanvas();
      this.createCanvas();
    }
  }
}

const canvasReducer = (state: CanvasState, action: SpectrumCanvasAction) => {
  state.clearCanvas();
  const nextState = state.copy();
  switch (action.type) {
    case CanvasActionType.SetData: {
      nextState.spectrumData = action.data;
      if (!nextState.isCanvasCompatibleWithData() && nextState.spectrumData) {
        nextState.createCanvas();
      }
      if (nextState.spectrumData) nextState.renderCanvas();
      break;
    }
    case CanvasActionType.RenderCanvas: {
      if (nextState.spectrumData) nextState.renderCanvas();
      else nextState.clearCanvas();
      break;
    }
    case CanvasActionType.ResizeCanvas: {
      nextState.windowSize = {height: action.height, width: action.width}
      nextState.createCanvas();
      if (nextState.spectrumData) nextState.renderCanvas();
      break;
    }
    case CanvasActionType.ToggleFeatureProfiles: {
      nextState.showFeatureProfiles = !nextState.showFeatureProfiles;
      // Force canvas creation
      nextState.createCanvas();
      if (nextState.spectrumData) nextState.renderCanvas();
      break;
    }
  }
  return nextState;
};

export function SpectrumCanvasComponent() {
  const canvasHolder = React.useRef<HTMLDivElement | null>(null);

  const [state, dispatch] = React.useReducer(
    canvasReducer,
    CanvasState.createEmpty(canvasHolder)
  );

  const viewerState = useSpectrumViewer();
  const spectrumData = viewerState.spectrumData;

  const isMobile = useMediaQuery("(max-width:500px)");

  React.useEffect(() => {
    function handleResize() {
      const dims = getWindowDimensions()
      dispatch({type: CanvasActionType.ResizeCanvas, ...dims})
    }

    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);


  React.useEffect(() => {
    dispatch({ type: CanvasActionType.SetData, data: spectrumData });
  }, [spectrumData]);
  return (
    <div>
      {isMobile && <>foobar</>}
      <div className="spectrum-view-container">
        <div
          className="spectrum-canvas"
          id={`spectrum-canvas-container-${state.id}`}
          ref={canvasHolder}
        />
      </div>
    </div>
  );
}
