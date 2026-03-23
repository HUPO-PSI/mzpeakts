import * as Arrow from "apache-arrow";
import * as ArrowFFI from "arrow-js-ffi";
import { ParquetFile, wasmMemory } from "parquet-wasm";

import { binarySearch, binarySearchAll } from "./utils";
import { bigIntToNumber } from "apache-arrow/util/bigint";
import { SpacingInterpolationModel, DataArrays } from "./data";

export class Param {
  name: string;
  value: any | null = null;
  accession: string | null = null;
  unit: string | null = null;

  constructor(
    name: string,
    value: any | null,
    accession: string | null = null,
    unit: string | null = null,
  ) {
    this.name = name;
    this.value = value;
    this.accession = accession;
    this.unit = unit;
  }

  static fromJSON(raw: any) {
    return new Param(raw.name, raw.value, raw.accession, raw.unit);
  }

  static fromArrow(array: Arrow.Vector) {
    const names = array.getChild("name") as Arrow.Vector<Arrow.Utf8>;
    const accessions = array.getChild("accession") as Arrow.Vector<Arrow.Utf8>;
    const units = array.getChild("unit") as Arrow.Vector<Arrow.Utf8>;
    const values = array.getChild("value") as Arrow.Vector<Arrow.Struct>;

    if (names == null || accessions == null || units == null || values == null)
      throw new Error(`Cannot convert ${array} to Param array`);
    return Array.from(names).map((name, i) => {
      if (name == null) throw new Error(`A Param name cannot be null`);
      const acc = accessions.get(i);
      const unit = units.get(i);
      const val = values.get(i);
      if (val == null) {
        return new this(name, val, acc, unit);
      } else {
        const typedVal = Object.values(val.toJSON()).filter((v) => v != null);
        let valueFor = null;
        if (typedVal.length) {
          valueFor = typedVal[0];
        }
        return new this(name, valueFor, acc, unit);
      }
    });
  }
}

export class SourceFile {
  id: string;
  name: string;
  location: string;
  parameters: Param[];

  constructor(id: string, name: string, location: string, parameters: Param[]) {
    this.id = id;
    this.name = name;
    this.location = location;
    this.parameters = parameters;
  }

  static fromJSON(raw: any) {
    const parameters = (raw.parameters as Array<any>).map(Param.fromJSON);
    return new SourceFile(raw.id, raw.name, raw.location, parameters);
  }
}

export class FileDescription {
  contents: Param[];
  sourceFiles: SourceFile[];

  constructor(contents: Param[], sourceFiles: SourceFile[]) {
    this.contents = contents;
    this.sourceFiles = sourceFiles;
  }

  static fromJSON(raw: any) {
    const contents = (raw.contents as Array<any>).map(Param.fromJSON);
    const sourceFiles = (raw.source_files as Array<any>).map(
      SourceFile.fromJSON,
    );
    return new FileDescription(contents, sourceFiles);
  }
}

export interface InstrumentComponent {
  componentType: string;
  order: number;
  parameters: Param[];
}

export interface InstrumentConfiguration {
  id: number;
  components: InstrumentComponent[];
  softwareReference?: string;
  parameters: Param[];
}

export interface Software {
  id: string;
  version: string;
  parameters: Param[];
}

export class FileMetadata {
  fileDescription: FileDescription;
  instrumentConfigurations: InstrumentConfiguration[];
  software: Software[];
  samples: any[];
  dataProcessingMethods: any[];
  run: any;

  constructor(
    fileDescription: FileDescription,
    instrumentConfigurations: InstrumentConfiguration[],
    software: Software[],
    samples: any[],
    dataProcessingMethods: any[],
    run: any,
  ) {
    this.fileDescription = fileDescription;
    this.instrumentConfigurations = instrumentConfigurations ?? [];
    this.software = software ?? [];
    this.samples = samples ?? [];
    this.dataProcessingMethods = dataProcessingMethods ?? [];
    this.run = run;
  }

  static fromParquet(handle: ParquetFile) {
    // TODO
    const meta = handle.metadata().fileMetadata().keyValueMetadata();
    // const keys = [
    //   "file_description",
    //   "instrument_configuration_list",
    //   "data_processing_method_list",
    //   "software_list",
    //   "sample_list",
    //   "run",
    // ];

    let raw = meta.get("file_description");
    let fileDescription = raw
      ? FileDescription.fromJSON(JSON.parse(raw))
      : new FileDescription([], []);

    return new FileMetadata(fileDescription, [], [], [], [], null);
  }
}

abstract class MetadataReaderBase {
  handle: ParquetFile;
  initialized: boolean = false;
  _iteratorHelpers: IteratorLookupTables | null = null;

  constructor(handle: ParquetFile) {
    this.handle = handle;
    this.initialized = false;
  }

  abstract makeIteratorHelpers(): IteratorLookupTables;

  protected get _mainStruct(): Arrow.Vector<Arrow.Struct> | null {
    throw new Error("Most override");
  }

  async init(): Promise<this> {
    throw new Error("Must override init");
  }

  get length(): number {
    return this.initialized && this._mainStruct ? this._mainStruct.length : 0;
  }

  protected async readTable() {
    const tab = await this.handle.read();
    const ffi = tab.intoFFI();
    const mem = wasmMemory();
    const arrowTab = ArrowFFI.parseTable(
      mem.buffer,
      ffi.arrayAddrs(),
      ffi.schemaAddr(),
      true,
    );
    ffi.free();
    return arrowTab;
  }
}

type IteratorLookupTables = Record<string, Map<bigint, HasSourceIndex[]>>;

interface HasSourceIndex {
  source_index: bigint | null;
  parameters?: Arrow.Vector | Param[];
}

const coerceToBasicRecordInTable = <T extends HasSourceIndex>(
  table: Map<bigint, T[]>,
  rec: T | undefined,
) => {
  if (
    rec === undefined ||
    rec.source_index === undefined ||
    rec.source_index === null
  )
    return;
  const c = table.get(rec.source_index as bigint);
  if (rec.parameters != undefined) {
    rec.parameters = Param.fromArrow(rec.parameters as Arrow.Vector);
  }
  if (c == undefined || c == null) {
    table.set(rec.source_index, [rec]);
  } else {
    c.push(rec);
  }
};

const buildBasicRecordTable = <T extends HasSourceIndex>(
  array: Arrow.Vector<Arrow.Struct>,
  convert?: Function,
): Map<bigint, T[]> => {
  const table = new Map();
  for (let i = 0; i < array.length; i++) {
    if (array.isValid(i)) {
      let rec = array.get(i)?.toJSON() as HasSourceIndex | undefined;
      if (convert != undefined) {
        rec = convert(rec);
      }
      coerceToBasicRecordInTable(table, rec);
    }
  }
  return table;
};

export class Scan {
  sourceIndex: bigint;
  instrumentConfigurationRef: number;
  params: Param[];
  scanWindows: any[];
  injectionTime?: number;
  presetScanConfiguration?: number;
  meta: any | null;

  constructor(
    sourceIndex: bigint,
    instrumentConfigurationRef: number,
    params: Param[],
    scanWindows?: any[],
    injectionTime?: number,
    presetScanConfiguration?: number,
    meta?: any,
  ) {
    this.sourceIndex = sourceIndex;
    this.instrumentConfigurationRef = instrumentConfigurationRef;
    this.params = params;
    this.scanWindows = scanWindows ?? [];
    this.injectionTime = injectionTime;
    this.presetScanConfiguration = presetScanConfiguration;
    this.meta = meta;
  }

  static fromRecord(record: any) {
    return new Scan(
      record.source_index,
      record.instrument_configuration_ref,
      record.parameters,
      Array.from(record.scan_windows).map((w: any) => w.toJSON()),
      record["MS_1000927_ion_injection_time_unit_UO_0000028"],
      record["MS_1000616_preset_scan_configuration"],
      record,
    );
  }
}

export class IsolationWindow {
  target: number;
  lowerOffset: number;
  upperOffset: number;

  constructor(target: number, lower: number, upper: number) {
    this.target = target;
    this.lowerOffset = lower;
    this.upperOffset = upper;
  }

  static fromRecord(record: any) {
    return new IsolationWindow(
      record["MS_1000827_isolation_window_target_mz"],
      record["MS_1000828_isolation_window_lower_offset"],
      record["MS_1000829_isolation_window_upper_offset"],
    );
  }
}

export class Precursor {
  sourceIndex: bigint;
  precursorIndex: bigint;
  activation: Param[];
  isolationWindow: IsolationWindow;
  meta: any;

  constructor(
    sourceIndex: bigint,
    precursorIndex: bigint,
    activation: Param[],
    isolationWindow: IsolationWindow,
    meta?: any,
  ) {
    this.sourceIndex = sourceIndex;
    this.precursorIndex = precursorIndex;
    this.activation = activation;
    this.isolationWindow = isolationWindow;
    this.meta = meta;
  }

  static fromRecord(record: any) {
    const activation = record.activation.parameters;
    return new Precursor(
      record.source_index,
      record.prescursor_index,
      activation,
      IsolationWindow.fromRecord(record.isolation_window),
      record,
    );
  }
}

export class SelectedIon {
  sourceIndex: bigint;
  precursorIndex: bigint;
  chargeState: number | null;
  intensity: number | null;
  mz: number | null;
  ionMobility: number | null;
  parameters: Param[];
  meta: any;

  constructor(
    sourceIndex: bigint,
    precursorIndex: bigint,
    mz?: number,
    intensity?: number,
    chargeState?: number,
    ionMobility?: number,
    parameters?: Param[],
    meta?: any,
  ) {
    this.sourceIndex = sourceIndex;
    this.precursorIndex = precursorIndex;
    this.chargeState = chargeState ?? null;
    this.mz = mz ?? null;
    this.intensity = intensity ?? null;
    this.ionMobility = ionMobility ?? null;
    this.parameters = parameters ?? [];
    this.meta = meta;
  }

  static fromRecord(record: any) {
    const parameters = record.parameters.map(Param.fromArrow);
    return new SelectedIon(
      record.source_index,
      record.precursor_index,
      record["MS_1000744_selected_ion_mz_unit_MS_1000040"],
      record["MS_1000042_intensity_unit_MS_1000131"],
      record["MS_1000041_charge_state"],
      record["ion_mobility"],
      parameters,
      record,
    );
  }
}

export class Spectrum {
  id: string;
  index: bigint;
  msLevel: number;
  isProfile: boolean;
  polarity: number;
  time: number;
  params: Param[];
  scans: any[];
  precursors: Precursor[];
  selectedIons: any[];
  meta: any | null;
  dataArrays?: DataArrays;

  constructor(
    id: string,
    index: bigint,
    msLevel: number,
    isProfile: boolean,
    polarity: number,
    time: number,
    params: Param[],
    scans?: any[],
    precursors?: any[],
    selectedIons?: any[],
    meta?: any | null,
    dataArrays?: DataArrays,
  ) {
    this.id = id;
    this.index = index;
    this.msLevel = msLevel;
    this.isProfile = isProfile;
    this.polarity = polarity;
    this.time = time;
    this.params = params;
    this.scans = scans ?? [];
    this.precursors = precursors ?? [];
    this.selectedIons = selectedIons ?? [];
    this.meta = meta ?? null;
    this.dataArrays = dataArrays;
  }

  static fromRecord(record: any) {
    return new Spectrum(
      record.id,
      record.index,
      record["MS_1000511_ms_level"],
      record["MS_1000525_spectrum_representation"] == "MS:1000128",
      record["MS_1000465_scan_polarity"],
      record["time"],
      record["parameters"],
      record["scans"].map(Scan.fromRecord),
      (record["precursors"] ?? []).map(Precursor.fromRecord),
      (record["selectedIons"] ?? []).map(SelectedIon.fromRecord),
      record,
      record.dataArrays,
    );
  }
}

export class SpectrumMetadata extends MetadataReaderBase {
  _spectra: Arrow.Vector<Arrow.Struct> | null;
  _scans: Arrow.Vector<Arrow.Struct> | null;
  _precursors: Arrow.Vector<Arrow.Struct> | null;
  _selectedIons: Arrow.Vector<Arrow.Struct> | null;

  constructor(handle: ParquetFile) {
    super(handle);
    this._spectra = null;
    this._scans = null;
    this._precursors = null;
    this._selectedIons = null;
  }

  fileMetadata() {
    return FileMetadata.fromParquet(this.handle);
  }

  static async fromParquet(handle: ParquetFile) {
    const self = new this(handle);
    return await self.init();
  }

  makeIteratorHelpers(): IteratorLookupTables {
    const lookups: IteratorLookupTables = {};
    if (this.scans) {
      lookups["scans"] = buildBasicRecordTable(this.scans);
    }
    if (this.precursors) {
      lookups["precursors"] = buildBasicRecordTable(this.precursors);
    }
    if (this.selectedIons) {
      lookups["selectedIons"] = buildBasicRecordTable(this.selectedIons);
    }
    return lookups;
  }

  protected get _mainStruct() {
    return this._spectra;
  }

  async init() {
    if (this.initialized) return this;
    const arrowTab = await this.readTable();
    this._spectra = arrowTab.getChild(
      "spectrum",
    ) as Arrow.Vector<Arrow.Struct> | null;
    this._scans = arrowTab.getChild(
      "scan",
    ) as Arrow.Vector<Arrow.Struct> | null;
    this._precursors = arrowTab.getChild(
      "precursor",
    ) as Arrow.Vector<Arrow.Struct> | null;
    this._selectedIons = arrowTab.getChild(
      "selected_ion",
    ) as Arrow.Vector<Arrow.Struct> | null;
    this.initialized = true;
    return this;
  }

  loadSpacingModelIndex(): Map<bigint, SpacingInterpolationModel> | null {
    if (this.spectra === null) return null;
    const indexArr = this.spectra.getChildAt(0) as Arrow.Vector<Arrow.Uint64>;
    const spacingModels = this.spectra.getChild(
      "mz_delta_model",
    ) as Arrow.Vector<Arrow.List<Arrow.Float64>> | null;
    if (spacingModels == null) return null;
    const modelIndex = new Map();
    for (let i = 0; i < indexArr.length; i++) {
      const key = indexArr.get(i);
      if (key == null) continue;
      const coefs = spacingModels.get(i);
      if (coefs == null) continue;
      const model = SpacingInterpolationModel.fromArrow(coefs);
      modelIndex.set(key, model);
    }
    return modelIndex;
  }

  get spectra() {
    if (!this.initialized)
      throw new Error("Metadata not initialized yet! call and await `init()`");
    return this._spectra;
  }

  get scans() {
    if (!this.initialized)
      throw new Error("Metadata not initialized yet! call and await `init()`");
    return this._scans;
  }

  get precursors() {
    if (!this.initialized)
      throw new Error("Metadata not initialized yet! call and await `init()`");
    return this._precursors;
  }

  get selectedIons() {
    if (!this.initialized)
      throw new Error("Metadata not initialized yet! call and await `init()`");
    return this._selectedIons;
  }

  get(index: number | bigint) {
    if (index >= this.length) throw new Error("Index out of range");
    let index_ = bigIntToNumber(index);
    let index_n = BigInt(index);
    if (this.spectra == null) throw new Error("Invalid state");

    let indexArr = this.spectra?.getChild(
      "index",
    ) as Arrow.Vector<Arrow.Uint64>;
    let row = indexArr.get(index_);
    if (row != index_n) {
      const offset = binarySearch(indexArr, index_n);
      row = indexArr.get(offset);
    }
    const spectrumRecord = this.spectra.get(index_)?.toJSON();
    if (!spectrumRecord)
      throw new Error("Invalid state, spectrum record not found");
    spectrumRecord.parameters = Param.fromArrow(spectrumRecord.parameters);

    indexArr = this.scans?.getChild(
      "source_index",
    ) as Arrow.Vector<Arrow.Uint64>;
    let offsets = binarySearchAll(indexArr, index_n);

    if (offsets && this.scans) {
      const scanRecords = Array.from(
        this.scans.slice(offsets[0], offsets[1]),
      ).map((e) => {
        if (!e) return e;
        const conv = e.toJSON();
        conv.parameters = Param.fromArrow(conv.parameters);
        return conv;
      });
      spectrumRecord.scans = scanRecords;
    }

    if (this.precursors != null) {
      indexArr = this.precursors?.getChild(
        "source_index",
      ) as Arrow.Vector<Arrow.Uint64>;
      offsets = binarySearchAll(indexArr, index_n);
      if (offsets) {
        const precursorRecords = this.precursors.slice(offsets[0], offsets[1]);
        spectrumRecord.precursors = Array.from(precursorRecords).map((e) => {
          if (!e) return e;
          const conv = e.toJSON();
          conv.isolation_window = conv.isolation_window.toJSON();
          conv.activation = conv.activation.toJSON();
          conv.activation.parameters = Param.fromArrow(
            conv.activation.parameters,
          );
          return conv;
        });
      }
    }

    if (this.selectedIons != null) {
      indexArr = this.selectedIons?.getChild(
        "source_index",
      ) as Arrow.Vector<Arrow.Uint64>;
      offsets = binarySearchAll(indexArr, index_n);
      if (offsets) {
        const ionRecords = this.selectedIons
          .slice(offsets[0], offsets[1])
          .toJSON();
        spectrumRecord.selectedIons = Array.from(ionRecords).map((e) => {
          if (!e) return e;
          const conv = e.toJSON();
          conv.parameters = Param.fromArrow(conv.parameters);
          return conv;
        });
      }
    }

    return spectrumRecord;
  }
}

export class ChromatogramMetadata extends MetadataReaderBase {
  _chromatograms: Arrow.Vector | null;
  _precursors: Arrow.Vector | null;
  _selectedIons: Arrow.Vector | null;

  constructor(handle: ParquetFile) {
    super(handle);
    this._chromatograms = null;
    this._precursors = null;
    this._selectedIons = null;
  }

  makeIteratorHelpers(): IteratorLookupTables {
    const lookups: IteratorLookupTables = {};
    if (this.precursors) {
      lookups["precursors"] = buildBasicRecordTable(this.precursors);
    }
    if (this.selectedIons) {
      lookups["selectedIons"] = buildBasicRecordTable(this.selectedIons);
    }
    return lookups;
  }

  static async fromParquet(handle: ParquetFile) {
    const self = new this(handle);
    return await self.init();
  }

  async init() {
    if (this.initialized) return this;
    const arrowTab = await this.readTable();
    this._chromatograms = arrowTab.getChild("chromatogram");
    this._precursors = arrowTab.getChild("precursor");
    this._selectedIons = arrowTab.getChild("selected_ion");
    this.initialized = true;
    return this;
  }

  protected get _mainStruct() {
    return this.chromatograms;
  }

  get chromatograms() {
    if (!this.initialized)
      throw new Error("Metadata not initialized yet! call and await `init()`");
    return this._chromatograms;
  }

  get precursors() {
    if (!this.initialized)
      throw new Error("Metadata not initialized yet! call and await `init()`");
    return this._precursors;
  }

  get selectedIons() {
    if (!this.initialized)
      throw new Error("Metadata not initialized yet! call and await `init()`");
    return this._selectedIons;
  }

  get length(): number {
    return this.initialized && this._chromatograms
      ? this._chromatograms.length
      : 0;
  }

  get(index: number | bigint) {
    if (index >= this.length) throw new Error("Index out of range");
    let index_ = bigIntToNumber(index);
    if (this.chromatograms == null) throw new Error("Invalid state");
    const indexArr = this.chromatograms?.getChild(
      "index",
    ) as Arrow.Vector<Arrow.Uint64>;
    let row = indexArr.get(index_);
    if (row != BigInt(index)) {
      index_ = binarySearch(indexArr, BigInt(index));
    }
    return this.chromatograms.get(index_);
  }
}
