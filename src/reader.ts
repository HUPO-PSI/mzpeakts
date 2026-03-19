import { ZipStorage } from "./store"
import { SpectrumMetadata, ChromatogramMetadata } from './metadata';
import { HttpRangeReader, BlobReader } from "@zip.js/zip.js";
import { DataArraysReader } from "./data";
import { BufferContext } from "./array_index";

export class MZPeakReader<T> {
  store: ZipStorage<T>;
  spectrumMetadata: SpectrumMetadata | null = null;
  chromatogramMetadata: ChromatogramMetadata | null = null;
  initialized: boolean = false;

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

    this.initialized = true;
    return this;
  }

  async spectrumData() {
    if (!this.initialized) await this.init();
    const handle = await this.store.spectrumData();
    if (!handle) return null;
    return await DataArraysReader.fromParquet(handle, BufferContext.Spectrum);
  }

  async spectrumPeaks() {
    if (!this.initialized) await this.init();
    const handle = await this.store.spectrumPeaks();
    if (!handle) return null;
    return await DataArraysReader.fromParquet(handle, BufferContext.Spectrum);
  }

  async chromatogramData() {
    if (!this.initialized) await this.init()
    const handle = await this.store.chromatogramData();
    if (!handle) return null;
    return await DataArraysReader.fromParquet(handle, BufferContext.Chromatogram);
  }
}