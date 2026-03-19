import * as Arrow  from "apache-arrow";
import * as ArrowFFI from  "arrow-js-ffi"
import { ParquetFile, wasmMemory, FFIStream } from "parquet-wasm/bundler";

import { binarySearch, binarySearchAll } from "./utils";
import { bigIntToNumber } from "apache-arrow/util/bigint";

export class SpectrumMetadata {
  handle: ParquetFile;
  _spectra: Arrow.Vector | null;
  _scans: Arrow.Vector | null;
  _precursors: Arrow.Vector | null;
  _selectedIons: Arrow.Vector | null;
  initialized: boolean = false;
  _ffi: FFIStream | null = null;

  constructor(handle: ParquetFile) {
    this.handle = handle;
    this._spectra = null;
    this._scans = null;
    this._precursors = null;
    this._selectedIons = null;
    this.initialized = false;
    this._ffi = null;
  }

  static async fromParquet(handle: ParquetFile) {
    const self = new this(handle);
    return await self.init();
  }

  free() {
    if (this._ffi !== null) {
      this._ffi.free();
    }
  }

  async init() {
    if (this.initialized) return this;
    const tab = await this.handle.read();
    this._ffi = tab.intoFFI();
    const mem = wasmMemory();
    const arrowTab = ArrowFFI.parseTable(
      mem.buffer,
      this._ffi.arrayAddrs(),
      this._ffi.schemaAddr(),
    );
    this._spectra = arrowTab.getChild("spectrum");
    this._scans = arrowTab.getChild("scan");
    this._precursors = arrowTab.getChild("precursor");
    this._selectedIons = arrowTab.getChild("selected_ion");
    this.initialized = true;
    return this;
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

  get length() : number {
    return this.initialized && this._spectra ? this._spectra.length : 0;
  }

  getRecord(index: number | bigint) {
    if (index >= this.length) throw new Error("Index out of range")
    let index_ = bigIntToNumber(index);
    let index_n = BigInt(index)
    if (this.spectra == null) throw new Error("Invalid state");

    let indexArr = this.spectra?.getChild("index") as Arrow.Vector<Arrow.Uint64>;
    let row = indexArr.get(index_)
    if (row != index_n) {
        const offset = binarySearch(indexArr, index_n);
        row = indexArr.get(offset);
    }
    const spectrumRecord = this.spectra.get(index_).toJSON()

    indexArr = this.scans?.getChild("source_index") as Arrow.Vector<Arrow.Uint64>;
    let offsets = binarySearchAll(indexArr, index_n);

    if (offsets && this.scans) {
        const scanRecords = Array.from(this.scans.slice(offsets[0], offsets[1])).map(e => e.toJSON());
        spectrumRecord.scans = scanRecords;
    }

    if (this.precursors != null) {
        indexArr = this.precursors?.getChild(
          "source_index",
        ) as Arrow.Vector<Arrow.Uint64>;
        offsets = binarySearchAll(indexArr, index_n);
        if (offsets) {
          const scanRecords = this.precursors
            .slice(offsets[0], offsets[1]);
          spectrumRecord.precursors = Array.from(scanRecords).map(e => e.toJSON());
        }
    }

    if (this.selectedIons != null) {
      indexArr = this.selectedIons?.getChild(
        "source_index",
      ) as Arrow.Vector<Arrow.Uint64>;
      offsets = binarySearchAll(indexArr, index_n);
      if (offsets) {
        const scanRecords = this.selectedIons
          .slice(offsets[0], offsets[1])
          .toJSON();
        spectrumRecord.selectedIons = Array.from(scanRecords).map((e) =>
          e.toJSON(),
        );
      }
    }

    return spectrumRecord
  }

}

export class ChromatogramMetadata {
  handle: ParquetFile;
  _chromatograms: Arrow.Vector | null;
  _precursors: Arrow.Vector | null;
  _selectedIons: Arrow.Vector | null;
  initialized: boolean = false;
  _ffi: FFIStream | null = null;

  constructor(handle: ParquetFile) {
    this.handle = handle;
    this._chromatograms = null;
    this._precursors = null;
    this._selectedIons = null;
    this.initialized = false;
    this._ffi = null;
  }

  static async fromParquet(handle: ParquetFile) {
    const self = new this(handle);
    return await self.init();
  }

  free() {
    if (this._ffi !== null) {
      this._ffi.free();
    }
  }

  async init() {
    if (this.initialized) return this;
    const tab = await this.handle.read();
    this._ffi = tab.intoFFI();
    const mem = wasmMemory();
    const arrowTab = ArrowFFI.parseTable(
      mem.buffer,
      this._ffi.arrayAddrs(),
      this._ffi.schemaAddr(),
    );
    this._chromatograms = arrowTab.getChild("chromatogram");
    this._precursors = arrowTab.getChild("precursor");
    this._selectedIons = arrowTab.getChild("selected_ion");
    this.initialized = true;
    return this;
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

  getRecord(index: number | bigint) {
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
