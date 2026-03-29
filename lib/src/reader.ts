import { ZipStorage } from "./store"
import { SpectrumMetadata, ChromatogramMetadata, FileMetadata } from './metadata';
import { HttpRangeReader, BlobReader } from "@zip.js/zip.js";
import { DataArraysReader, packTableIntoDataArrays, packTableIntoPeaks } from "./data";
import { BufferContext } from "./array_index";
import { PointLike } from "./record";


export class MZPeakReader<T> {
  store: ZipStorage<T>;
  spectrumMetadata: SpectrumMetadata | null = null;
  chromatogramMetadata: ChromatogramMetadata | null = null;
  wavelengthMetadata: SpectrumMetadata | null = null;
  initialized: boolean = false;
  _spectrumDataReader: DataArraysReader | null = null;
  _spectrumPeaksReader: DataArraysReader | null = null;
  _chromatogramDataReader: DataArraysReader | null = null;
  _wavelengthSpectrumDataReader: DataArraysReader | null = null;
  _fileMetadata: FileMetadata | undefined = undefined

  constructor(store: ZipStorage<T>) {
    this.store = store;
  }

  get fileMetadata() {
    if (this._fileMetadata != undefined) return this._fileMetadata
    this._fileMetadata = this.spectrumMetadata?.fileMetadata()
    return this._fileMetadata
  }

  static async fromStore<T>(store: ZipStorage<T>) {
    const self = new this(store);
    await self.init();
    return self;
  }

  static async fromUrl(url: string | URL) {
    return await MZPeakReader.fromStore(
      new ZipStorage(new HttpRangeReader(url)),
    );
  }

  static async fromBlob(blob: Blob) {
    return await MZPeakReader.fromStore(new ZipStorage(new BlobReader(blob)));
  }

  async init() {
    if (this.initialized) return this;
    await this.store.init();
    const spectrumMetaHandle = await this.store.spectrumMetadata();
    if (spectrumMetaHandle) {
      this.spectrumMetadata =
        await SpectrumMetadata.fromParquet(spectrumMetaHandle);
    }

    const chromatogramMetaHandle = await this.store.chromatogramMetadata();
    if (chromatogramMetaHandle) {
      this.chromatogramMetadata = await ChromatogramMetadata.fromParquet(
        chromatogramMetaHandle,
      );
    }

    const wavelengthMetadataHandle =
      await this.store.wavelengthSpectrumMetadata();
    if (wavelengthMetadataHandle) {
      this.wavelengthMetadata = await SpectrumMetadata.fromParquet(
        wavelengthMetadataHandle,
      );
    }

    this.initialized = true;
    return this;
  }

  async spectrumData() {
    if (this._spectrumDataReader) return this._spectrumDataReader;
    if (!this.initialized) await this.init();
    const handle = await this.store.spectrumData();
    if (!handle) return null;
    const dataReader = await DataArraysReader.fromParquet(
      handle,
      BufferContext.Spectrum,
    );
    if (this.spectrumMetadata)
      dataReader.spacingModels = this.spectrumMetadata.loadSpacingModelIndex();
    this._spectrumDataReader = dataReader;
    return dataReader;
  }

  async *enumerateSpectra() {
    if (!this.spectrumMetadata) return;
    const dataReader = await this.spectrumData();
    if (!dataReader) return;
    const it = dataReader.enumerate();
    let n = this.spectrumMetadata.length;
    for (let i = 0; i < n; i++) {
      const meta = this.spectrumMetadata.get(i);
      await it.seek(BigInt(i));
      let { done, value: data } = await it.next();
      if (done) break;
      data = data[1];
      if (data) {
        meta["dataArrays"] = packTableIntoDataArrays(data);
      }
      yield meta;
    }
  }

  async spectrumPeaks() {
    if (this._spectrumPeaksReader) return this._spectrumPeaksReader;
    if (!this.initialized) await this.init();
    const handle = await this.store.spectrumPeaks();
    if (!handle) return null;
    return await DataArraysReader.fromParquet(handle, BufferContext.Spectrum);
  }

  async chromatogramData() {
    if (this._chromatogramDataReader) return this._chromatogramDataReader;
    if (!this.initialized) await this.init();
    const handle = await this.store.chromatogramData();
    if (!handle) return null;
    this._chromatogramDataReader = await DataArraysReader.fromParquet(
      handle,
      BufferContext.Chromatogram,
    );
    return this._chromatogramDataReader;
  }

  async wavelengthSpectrumData() {
    if (this._wavelengthSpectrumDataReader)
      return this._wavelengthSpectrumDataReader;
    if (!this.initialized) await this.init();
    const handle = await this.store.wavelengthSpectrumData();
    if (!handle) return null;
    this._wavelengthSpectrumDataReader = await DataArraysReader.fromParquet(
      handle,
      BufferContext.Spectrum,
    );
    return this._wavelengthSpectrumDataReader;
  }

  async getSpectrum(index_: bigint | number) {
    const index = BigInt(index_);
    const meta = this.spectrumMetadata?.get(index);
    if (meta) {
      const handle = await this.spectrumData();
      const data = await handle?.get(index);
      if (data) {
        meta["dataArrays"] = packTableIntoDataArrays(data);
      }
      const peakHandle = await this.spectrumPeaks();
      const peakData = await peakHandle?.get(index);
      if (peakData && peakData.numRows > 0) {
        const peaks = packTableIntoPeaks(peakData) as any as PointLike[];
        meta.centroids = peaks
      }
      return meta;
    }
  }

  get numSpectra() {
    return this.spectrumMetadata?.length ?? 0;
  }

  async getChromatogram(index_: bigint | number) {
    const index = BigInt(index_);
    const meta = this.chromatogramMetadata?.get(index);
    if (meta) {
      const handle = await this.chromatogramData();
      const data = await handle?.get(index);
      if (data) {
        meta["dataArrays"] = packTableIntoDataArrays(data);
      }
      return meta;
    }
  }

  get numChromatograms() {
    return this.chromatogramMetadata?.length ?? 0;
  }

  async getWavelengthSpectrum(index_: bigint | number) {
    const index = BigInt(index_);
    const meta = this.wavelengthMetadata?.get(index);
    if (meta) {
      const handle = await this.wavelengthSpectrumData();
      const data = await handle?.get(index);
      if (data) {
        meta["dataArrays"] = packTableIntoDataArrays(data);
      }
      return meta;
    }
  }

  get numWavelengthSpectra() {
    return this.wavelengthMetadata?.length ?? 0;
  }

  async at(index: bigint | number) {
    return await this.getSpectrum(index);
  }

  async get(index: bigint | number) {
    return await this.getSpectrum(index);
  }

  get length() {
    return this.numSpectra;
  }

  async *[Symbol.asyncIterator]() {
    for await (let value of this.enumerateSpectra()) {
      yield value
    }
  }
}