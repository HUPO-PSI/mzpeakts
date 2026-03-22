import { ZipStorage } from "./store"
import { SpectrumMetadata, ChromatogramMetadata } from './metadata';
import { HttpRangeReader, BlobReader } from "@zip.js/zip.js";
import { DataArraysReader } from "./data";
import { BufferContext } from "./array_index";

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

  constructor(store: ZipStorage<T>) {
    this.store = store;
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
    const dataReader = await DataArraysReader.fromParquet(handle, BufferContext.Spectrum);
    if (this.spectrumMetadata)
      dataReader.spacingModels = this.spectrumMetadata.loadSpacingModelIndex()
    this._spectrumDataReader = dataReader
    return dataReader
  }

  async *enumerateSpectra() {
    if (!this.spectrumMetadata) return;
    const dataReader = await this.spectrumData();
    if (!dataReader) return;
    const it = dataReader?.enumerate();
    let n = this.spectrumMetadata.length;
    for(let i = 0; i < n; i++) {
      const meta = this.spectrumMetadata.get(i)
      await it.seek(BigInt(i))
      let {done, value: data} = await it.next();
      if (done) return;
      data = data[1]
      if (data) {
        console.log(data)
        for (let i = 1; i < data.schema.fields.length; i++) {
          const colName = data.schema.fields[i].name;
          meta[colName] = data.getChildAt(i)?.toArray();
        }
      }
      return meta
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
    if (this._wavelengthSpectrumDataReader) return this._wavelengthSpectrumDataReader;
    if (!this.initialized) await this.init();
    const handle = await this.store.wavelengthSpectrumData();
    if (!handle) return null;
    this._wavelengthSpectrumDataReader = await DataArraysReader.fromParquet(handle, BufferContext.Spectrum);
    return this._wavelengthSpectrumDataReader;
  }

  async getSpectrum(index: bigint) {
    const meta = this.spectrumMetadata?.get(index);
    if (meta) {
      const handle = await this.spectrumData();
      const data = await handle?.get(index);
      if (data) {
        for (let i = 1; i < data.schema.fields.length; i++) {
          const colName = data.schema.fields[i].name;
          meta[colName] = data.getChildAt(i)?.toArray();
        }
      }
      return meta;
    }
  }

  get numSpectra() {
    return this.spectrumMetadata?.length ?? 0;
  }

  async getChromatogram(index: bigint) {
    const meta = this.chromatogramMetadata?.get(index);
    if (meta) {
      const handle = await this.chromatogramData();
      const data = await handle?.get(index);
      if (data) {
        for (let i = 1; i < data.schema.fields.length; i++) {
          const colName = data.schema.fields[i].name;
          meta[colName] = data.getChildAt(i)?.toArray();
        }
      }
      return meta;
    }
  }

  get numChromatograms() {
    return this.chromatogramMetadata?.length ?? 0;
  }

  async getWavelengthSpectrum(index: bigint) {
    const meta = this.wavelengthMetadata?.get(index);
    if (meta) {
      const handle = await this.wavelengthSpectrumData();
      const data = await handle?.get(index);
      if (data) {
        for (let i = 1; i < data.schema.fields.length; i++) {
          const colName = data.schema.fields[i].name;
          meta[colName] = data.getChildAt(i)?.toArray();
        }
      }
      return meta;
    }
  }

  get numWavelengthSpectra() {
    return this.wavelengthMetadata?.length ?? 0;
  }
}