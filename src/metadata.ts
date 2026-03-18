import * as Arrow  from "apache-arrow";
import * as ArrowFFI from  "arrow-js-ffi"
import { ParquetFile, wasmMemory, FFIStream } from "parquet-wasm/bundler";

import { binarySearch } from "./utils";

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

  get(index: number) {
    if (index >= this.length) throw new Error("Index out of range")
    let index_ = index;
    if (this.spectra == null) throw new Error("Invalid state");
    const indexArr = this.spectra?.getChild("index") as Arrow.Vector<Arrow.Uint64>;
    let row = indexArr.get(index_)
    if (row != BigInt(index)) {
        index_ = binarySearch(indexArr, BigInt(index));
        row = indexArr.get(index_);
    }
    return this.spectra.get(index_)
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
    return this.initialized && this._chromatograms ? this._chromatograms.length : 0;
  }

  get(index: number) {
    if (index >= this.length) throw new Error("Index out of range");
    let index_ = index;
    if (this.chromatograms == null) throw new Error("Invalid state");
    const indexArr = this.chromatograms?.getChild("index") as Arrow.Vector<Arrow.Uint64>;
    let row = indexArr.get(index_);
    if (row != BigInt(index)) {
      index_ = binarySearch(indexArr, BigInt(index));
    }
    return this.chromatograms.get(index_);
  }
}
