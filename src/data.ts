import * as Arrow from "apache-arrow";
import * as ArrowFFI from "arrow-js-ffi";
import { ParquetFile, wasmMemory } from "parquet-wasm/bundler";

import {
  ArrayIndex,
  ArrayIndexEntry,
  BufferContext,
  BufferFormat,
  bufferContextIndexName,
  bufferContextName,
} from "./array_index";

// ---- Stub types & encoding constants ----

export type SpacingInterpolationModel = { coefficients: number[] };

export const NULL_INTERPOLATE_CURIE = "MS:1003901";
export const NULL_ZERO_CURIE = "MS:1003902";

const NO_COMPRESSION_CURIE = "MS:1000576";
const DELTA_CURIE = "MS:1003089";
const NUMPRESS_LINEAR_CURIE = "MS:1002312";
const NUMPRESS_SLOF_CURIE = "MS:1002314";

// ---- Internal helpers ----

async function readArrowBatches(
  handle: ParquetFile,
  rowGroups?: number[],
): Promise<Arrow.RecordBatch[]> {
  const options: any = rowGroups != null ? { rowGroups } : {};
  const tab = await handle.read(options);
  const ffi = tab.intoFFI();
  const mem = wasmMemory();
  const arrowTab = ArrowFFI.parseTable(
    mem.buffer,
    ffi.arrayAddrs(),
    ffi.schemaAddr(),
  );
  ffi.free();
  return arrowTab.batches;
}

function rootStructOf(
  batch: Arrow.RecordBatch,
): Arrow.Vector<Arrow.Struct> | null {
  return batch.getChildAt(0) as Arrow.Vector<Arrow.Struct> | null;
}

function indexVectorOf(
  rootStruct: Arrow.Vector<Arrow.Struct>,
): Arrow.Vector<Arrow.Uint64> {
  return rootStruct.getChildAt(0) as Arrow.Vector<Arrow.Uint64>;
}

function listCellToNumbers(
  listVec: Arrow.Vector<Arrow.List<Arrow.DataType>>,
  rowIdx: number,
): number[] {
  const cell = listVec.get(rowIdx);
  if (cell == null) return [];
  const out = new Array<number>(cell.length);
  for (let j = 0; j < cell.length; j++) out[j] = Number(cell.get(j));
  return out;
}

function decodeNoCompression(startValue: number, values: number[]): number[] {
  const result = new Array<number>(values.length + 1);
  result[0] = startValue;
  for (let i = 0; i < values.length; i++) result[i + 1] = values[i];
  return result;
}

function decodeDelta(startValue: number, values: number[]): number[] {
  const result = new Array<number>(values.length + 1);
  result[0] = startValue;
  for (let i = 0; i < values.length; i++) result[i + 1] = result[i] + values[i];
  return result;
}

function interpolateNulls(
  values: (number | null)[],
  model: SpacingInterpolationModel,
): number[] {
  const [a, b] = model.coefficients;
  return values.map((v, i) => (v != null ? v : a + b * i));
}

function mergeColumns(
  target: Record<string, unknown[]>,
  source: Record<string, unknown[]>,
) {
  for (const [key, values] of Object.entries(source)) {
    if (!target[key]) target[key] = [];
    target[key].push(...values);
  }
}

// ---- GroupTagBounds ----

export class GroupTagBounds {
  key: bigint;
  start: bigint;
  end: bigint;

  constructor(key: bigint, start: bigint, end: bigint) {
    this.key = key;
    this.start = start;
    this.end = end;
  }

  contains(value: bigint): boolean {
    const value_ = BigInt(value)
    return this.start <= value_ && value_ <= this.end;
  }

}

// ---- RangeIndex ----

export class RangeIndex implements Iterable<GroupTagBounds> {
  ranges: GroupTagBounds[];

  constructor(ranges: GroupTagBounds[]) {
    this.ranges = ranges;
  }

  get length(): number {
    return this.ranges.length;
  }

  [Symbol.iterator](): Iterator<GroupTagBounds> {
    return this.ranges[Symbol.iterator]();
  }

  findByKey(key: bigint): GroupTagBounds | null {
    const key_ = BigInt(key);
    if (this.length === 0) return null;
    let lo = 0,
      hi = this.ranges.length - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      const k = this.ranges[mid].key;
      if (k === key_) return this.ranges[mid];
      else if (k < key_) lo = mid + 1;
      else hi = mid - 1;
    }
    return null;
  }

  keysFor(index_: bigint): bigint[] {
    const index = BigInt(index_)
    return this.ranges.filter((r) => r.contains(index)).map((r) => r.key);
  }
}

// ---- DataArraysReaderMeta ----

export class DataArraysReaderMeta {
  context: BufferContext;
  arrayIndex: ArrayIndex;
  rowGroupIndex: RangeIndex;
  entrySpanIndex: RangeIndex;
  format: BufferFormat;
  spacingModels: Map<bigint, SpacingInterpolationModel> | null;

  constructor(
    context: BufferContext,
    arrayIndex: ArrayIndex,
    rowGroupIndex: RangeIndex,
    entrySpanIndex: RangeIndex,
    format: BufferFormat,
    spacingModels: Map<bigint, SpacingInterpolationModel> | null = null,
  ) {
    this.context = context;
    this.arrayIndex = arrayIndex;
    this.rowGroupIndex = rowGroupIndex;
    this.entrySpanIndex = entrySpanIndex;
    this.format = format;
    this.spacingModels = spacingModels;
  }

  static async fromParquet(
    handle: ParquetFile,
    context: BufferContext,
  ): Promise<DataArraysReaderMeta> {
    const pqMeta = handle.metadata();
    const nRowGroups = pqMeta.numRowGroups();
    if (nRowGroups === 0) throw new Error("Empty Parquet file");

    // 1. Load ArrayIndex JSON from file key-value metadata
    const kvMeta = pqMeta.fileMetadata().keyValueMetadata() as Map<
      string,
      string
    >;
    const arrayIndexKey = `${bufferContextName(context)}_array_index`;
    const arrayIndexJson = kvMeta.get(arrayIndexKey);
    if (arrayIndexJson == null)
      throw new Error(
        `Array index key "${arrayIndexKey}" missing from file metadata`,
      );
    const arrayIndex = ArrayIndex.fromJSON(JSON.parse(arrayIndexJson));

    // 2. Infer buffer format from first column path prefix
    const firstColPath = pqMeta.rowGroup(0).column(0).columnPath().join(".");
    let format: BufferFormat;
    if (firstColPath.startsWith("point")) format = BufferFormat.Point;
    else if (firstColPath.startsWith("chunk"))
      format = BufferFormat.ChunkValues;
    else
      throw new Error(`Root schema prefix "${firstColPath}" not recognized`);

    // 3. Annotate schema indices from row group 0 column paths
    const nCols = pqMeta.rowGroup(0).numColumns();
    for (let i = 0; i < nCols; i++) {
      let pathOf = pqMeta.rowGroup(0).column(i).columnPath().join(".");
      if (pathOf.endsWith(".list.item"))
        pathOf = pathOf.slice(0, -".list.item".length);
      else if (pathOf.endsWith(".list.element"))
        pathOf = pathOf.slice(0, -".list.element".length);
      for (const entry of arrayIndex.entries) {
        if (entry.path === pathOf) entry.schemaIndex = i;
      }
    }

    // 4. Build RangeIndex and EntrySpanIndex by scanning index column per row group
    const idxColName = bufferContextIndexName(context);
    const rowGroupBounds: GroupTagBounds[] = [];
    const rowSpanBounds: GroupTagBounds[] = [];
    let offset = 0n;
    let lastIdx = 0n;
    let spanStart = 0n;
    let seenFirst = false;

    for (let i = 0; i < nRowGroups; i++) {
      const batches = await readArrowBatches(handle, [i]);
      let rgMin: bigint | null = null;
      let rgMax: bigint | null = null;

      for (const batch of batches) {
        const rootStruct = rootStructOf(batch);
        if (rootStruct == null) {
          offset += BigInt(batch.numRows);
          continue;
        }
        const indexVec = rootStruct.getChild(idxColName) as
          | Arrow.Vector<Arrow.Uint64>
          | null;
        if (indexVec == null) {
          offset += BigInt(rootStruct.length);
          continue;
        }

        for (let r = 0; r < indexVec.length; r++) {
          const srcIdx = indexVec.get(r);
          offset += 1n;
          if (srcIdx == null) continue;
          if (rgMin == null || srcIdx < rgMin) rgMin = srcIdx;
          if (rgMax == null || srcIdx > rgMax) rgMax = srcIdx;
          if (!seenFirst) {
            lastIdx = srcIdx;
            spanStart = offset - 1n;
            seenFirst = true;
          } else if (srcIdx !== lastIdx) {
            rowSpanBounds.push(
              new GroupTagBounds(lastIdx, spanStart, offset - 1n),
            );
            lastIdx = srcIdx;
            spanStart = offset;
          }
        }
      }

      if (rgMin != null && rgMax != null) {
        rowGroupBounds.push(new GroupTagBounds(BigInt(i), rgMin, rgMax));
      }
    }
    if (seenFirst) {
      rowSpanBounds.push(new GroupTagBounds(lastIdx, spanStart, offset - 1n));
    }

    return new DataArraysReaderMeta(
      context,
      arrayIndex,
      new RangeIndex(rowGroupBounds),
      new RangeIndex(rowSpanBounds),
      format,
    );
  }
}

// ---- Layout readers ----

type ColumnMap = Record<string, unknown[]>;

export class BaseLayoutReader {
  protected arrayIndex: ArrayIndex;
  protected batches: Arrow.RecordBatch[];
  protected spacingModels: Map<bigint, SpacingInterpolationModel> | undefined;

  constructor(
    batches: Arrow.RecordBatch[],
    arrayIndex: ArrayIndex,
    spacingModels?: Map<bigint, SpacingInterpolationModel>,
  ) {
    this.batches = batches;
    this.arrayIndex = arrayIndex;
    this.spacingModels = spacingModels;
  }

  processSelectedRows(
    _entryIndex: bigint,
    rootStruct: Arrow.Vector<Arrow.Struct>,
    selectedRows: number[],
  ): ColumnMap {
    const result: ColumnMap = {};
    for (const field of rootStruct.type.children) {
      const vec = rootStruct.getChild(field.name)!;
      result[field.name] = selectedRows.map((i) => vec.get(i));
    }
    return result;
  }

  processRows(
    entryIndex: bigint,
    rootStruct: Arrow.Vector<Arrow.Struct>,
  ): ColumnMap {
    const idxVec = indexVectorOf(rootStruct);
    const selectedRows: number[] = [];
    for (let i = 0; i < idxVec.length; i++) {
      if (idxVec.get(i) === entryIndex) selectedRows.push(i);
    }
    return this.processSelectedRows(entryIndex, rootStruct, selectedRows);
  }

  async readRowsOf(
    entryIndex: bigint,
    startFrom: bigint,
    endAt: bigint,
  ): Promise<Arrow.Table> {
    let rowCountRead = 0n;
    const accumulated: ColumnMap = {};

    for (const batch of this.batches) {
      const rootStruct = rootStructOf(batch);
      if (rootStruct == null) {
        rowCountRead += BigInt(batch.numRows);
        continue;
      }

      const batchSize = BigInt(rootStruct.length);
      if (rowCountRead + batchSize < startFrom) {
        rowCountRead += batchSize;
        continue;
      }
      if (rowCountRead >= endAt) break;

      mergeColumns(accumulated, this.processRows(entryIndex, rootStruct));
      rowCountRead += batchSize;
    }

    return Arrow.tableFromArrays(accumulated as any);
  }
}

export class PointLayoutReader extends BaseLayoutReader {
  override processSelectedRows(
    entryIndex: bigint,
    rootStruct: Arrow.Vector<Arrow.Struct>,
    selectedRows: number[],
  ): ColumnMap {
    const base = super.processSelectedRows(
      entryIndex,
      rootStruct,
      selectedRows,
    );

    for (const entry of this.arrayIndex.entries) {
      const fieldName = entry.path.split(".").pop()!;
      if (!(fieldName in base)) continue;

      if (entry.transform === NULL_ZERO_CURIE) {
        base[fieldName] = (base[fieldName] as (number | null)[]).map(
          (v) => v ?? 0,
        );
      } else if (
        entry.transform === NULL_INTERPOLATE_CURIE &&
        this.spacingModels?.has(entryIndex)
      ) {
        base[fieldName] = interpolateNulls(
          base[fieldName] as (number | null)[],
          this.spacingModels.get(entryIndex)!,
        );
      }
    }

    return base;
  }
}

export class ChunkLayoutReader extends BaseLayoutReader {
  private mainAxisEntry: ArrayIndexEntry | null = null;
  private chunkStartFieldName = "";
  private chunkEncodingFieldName = "";
  private chunkValuesFieldName = "";
  private secondaryFields: { name: string; entry: ArrayIndexEntry }[] = [];

  constructor(
    batches: Arrow.RecordBatch[],
    arrayIndex: ArrayIndex,
    spacingModels?: Map<bigint, SpacingInterpolationModel>,
  ) {
    super(batches, arrayIndex, spacingModels);
    this.configureIndices();
  }

  private configureIndices() {
    for (const entry of this.arrayIndex.entries) {
      const fieldName = entry.path.split(".").pop()!;
      switch (entry.bufferFormat) {
        case BufferFormat.ChunkStart:
          this.chunkStartFieldName = fieldName;
          break;
        case BufferFormat.ChunkEncoding:
          this.chunkEncodingFieldName = fieldName;
          break;
        case BufferFormat.ChunkValues:
          this.mainAxisEntry = entry;
          this.chunkValuesFieldName = fieldName;
          break;
        case BufferFormat.ChunkSecondary:
        case BufferFormat.ChunkTransform:
          this.secondaryFields.push({ name: fieldName, entry });
          break;
      }
    }
    if (!this.chunkEncodingFieldName)
      throw new Error("Chunk encoding column not found");
    if (this.mainAxisEntry == null) throw new Error("Main axis cannot be null");
  }

  override processSelectedRows(
    entryIndex: bigint,
    rootStruct: Arrow.Vector<Arrow.Struct>,
    selectedRows: number[],
  ): ColumnMap {
    if (selectedRows.length === 0) return {};
    if (this.mainAxisEntry == null)
      throw new Error("Main axis cannot be null");

    const chunkStartVec = rootStruct.getChild(this.chunkStartFieldName)!;
    const chunkEncodingVec = rootStruct.getChild(
      this.chunkEncodingFieldName,
    ) as Arrow.Vector<Arrow.Utf8>;
    const chunkValuesVec = rootStruct.getChild(
      this.chunkValuesFieldName,
    ) as Arrow.Vector<Arrow.List<Arrow.DataType>>;

    const indexColName = bufferContextIndexName(this.mainAxisEntry.context);
    const mainAxisName = this.chunkValuesFieldName.replace("_chunk_values", "");

    const resultIndex: bigint[] = [];
    const resultMainAxis: number[] = [];
    const resultSecondary: Record<string, number[]> = {};
    for (const { name } of this.secondaryFields) resultSecondary[name] = [];

    for (const rowIdx of selectedRows) {
      const startValue = Number(chunkStartVec.get(rowIdx) ?? 0);
      const encoding = chunkEncodingVec.get(rowIdx) ?? "";
      const chunkValues = listCellToNumbers(chunkValuesVec, rowIdx);

      let decoded: number[];
      switch (encoding) {
        case NO_COMPRESSION_CURIE:
          decoded = decodeNoCompression(startValue, chunkValues);
          break;
        case DELTA_CURIE:
          decoded = decodeDelta(startValue, chunkValues);
          break;
        case NUMPRESS_LINEAR_CURIE:
        case NUMPRESS_SLOF_CURIE:
          throw new Error(
            `Numpress decoding not implemented (encoding: ${encoding})`,
          );
        default:
          throw new Error(`Unknown chunk encoding: ${encoding}`);
      }

      for (const v of decoded) {
        resultIndex.push(entryIndex);
        resultMainAxis.push(v);
      }

      for (const { name, entry } of this.secondaryFields) {
        const secVec = rootStruct.getChild(
          name,
        ) as Arrow.Vector<Arrow.List<Arrow.DataType>>;
        const secValues = listCellToNumbers(secVec, rowIdx);

        if (entry.transform === NULL_ZERO_CURIE) {
          resultSecondary[name].push(...secValues.map((v) => v || 0));
        } else if (
          entry.transform === NUMPRESS_SLOF_CURIE ||
          entry.transform === NUMPRESS_LINEAR_CURIE
        ) {
          throw new Error(
            `Numpress decoding not implemented for secondary axis (transform: ${entry.transform})`,
          );
        } else {
          resultSecondary[name].push(...secValues);
        }
      }
    }

    const result: ColumnMap = {
      [indexColName]: resultIndex,
      [mainAxisName]: resultMainAxis,
    };
    for (const [name, values] of Object.entries(resultSecondary)) {
      result[name] = values;
    }
    return result;
  }
}

// ---- DataArraysReader ----

export class DataArraysReader implements AsyncIterable<[bigint, Arrow.Table]> {
  bufferContext: BufferContext;
  handle: ParquetFile;
  metadata: DataArraysReaderMeta;

  constructor(handle: ParquetFile, meta: DataArraysReaderMeta) {
    this.handle = handle;
    this.metadata = meta;
    this.bufferContext = meta.context;
  }

  static async fromParquet(handle: ParquetFile, context: BufferContext) {
    const meta = await DataArraysReaderMeta.fromParquet(handle, context);
    return new this(handle, meta)
  }

  get arrayIndex(): ArrayIndex {
    return this.metadata.arrayIndex;
  }
  get rowGroupIndex(): RangeIndex {
    return this.metadata.rowGroupIndex;
  }
  get entrySpanIndex(): RangeIndex {
    return this.metadata.entrySpanIndex;
  }
  get format(): BufferFormat {
    return this.metadata.format;
  }
  get length(): number {
    return this.metadata.entrySpanIndex.length;
  }

  get spacingModels(): Map<bigint, SpacingInterpolationModel> | null {
    return this.metadata.spacingModels;
  }
  set spacingModels(v: Map<bigint, SpacingInterpolationModel> | null) {
    this.metadata.spacingModels = v;
  }

  makeLayoutReader(batches: Arrow.RecordBatch[]): BaseLayoutReader {
    const models = this.spacingModels ?? undefined;
    if (this.metadata.format === BufferFormat.Point) {
      return new PointLayoutReader(batches, this.metadata.arrayIndex, models);
    } else if (this.metadata.format === BufferFormat.ChunkValues) {
      return new ChunkLayoutReader(batches, this.metadata.arrayIndex, models);
    }
    throw new Error("Data layout not recognized");
  }

  async readForIndex(key_: bigint|number): Promise<Arrow.Table | null> {
    const key = BigInt(key_)
    const rowGroups = this.rowGroupIndex.keysFor(key);
    if (rowGroups.length === 0) return null;

    const rowSpan = this.entrySpanIndex.findByKey(key);
    if (rowSpan == null) return null;

    // Offset: total rows in row groups before the first relevant one
    let offset = 0n;
    const pqMeta = this.handle.metadata();
    const firstRg = Number(rowGroups[0]);
    for (let i = 0; i < firstRg; i++) {
      offset += BigInt(pqMeta.rowGroup(i).numRows());
    }

    const batches = await readArrowBatches(this.handle, rowGroups.map(Number));
    const layoutReader = this.makeLayoutReader(batches);
    return layoutReader.readRowsOf(key, rowSpan.start - offset, rowSpan.end - offset);
  }

  enumerate(): DataArraysIter {
    return new DataArraysIter(this);
  }

  [Symbol.asyncIterator](): AsyncIterator<[bigint, Arrow.Table]> {
    return this.enumerate();
  }
}

// ---- DataArraysIter ----

type EntrySection = { batchIdx: number; rows: number[] };
type EntryRecord = { index: bigint; sections: EntrySection[] };

export class DataArraysIter
  implements
    AsyncIterator<[bigint, Arrow.Table]>,
    AsyncIterable<[bigint, Arrow.Table]>
{
  private reader: DataArraysReader;
  private initialized = false;
  private allBatches: Arrow.RecordBatch[] = [];
  private layoutReader: BaseLayoutReader | null = null;
  private entries: EntryRecord[] = [];
  private entryPos = 0;
  private _current: [bigint, Arrow.Table] | null = null;

  constructor(reader: DataArraysReader) {
    this.reader = reader;
  }

  get current(): [bigint, Arrow.Table] {
    if (!this._current)
      throw new Error("Iterator not initialized or exhausted");
    return this._current;
  }

  private async initialize(): Promise<void> {
    this.allBatches = await readArrowBatches(this.reader.handle);
    this.layoutReader = this.reader.makeLayoutReader(this.allBatches);

    const entryMap = new Map<bigint, EntrySection[]>();

    for (let bi = 0; bi < this.allBatches.length; bi++) {
      const rootStruct = rootStructOf(this.allBatches[bi]);
      if (rootStruct == null) continue;

      const idxVec = indexVectorOf(rootStruct);
      const batchGroups = new Map<bigint, number[]>();

      for (let r = 0; r < idxVec.length; r++) {
        const idx = idxVec.get(r);
        if (idx == null) continue;
        if (!batchGroups.has(idx)) batchGroups.set(idx, []);
        batchGroups.get(idx)!.push(r);
      }

      for (const [idx, rows] of batchGroups) {
        if (!entryMap.has(idx)) entryMap.set(idx, []);
        entryMap.get(idx)!.push({ batchIdx: bi, rows });
      }
    }

    this.entries = Array.from(entryMap.entries())
      .sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))
      .map(([index, sections]) => ({ index, sections }));

    this.initialized = true;
  }

  private async moveNextAsync(): Promise<boolean> {
    if (!this.initialized) await this.initialize();
    if (this.entryPos >= this.entries.length) return false;

    const entry = this.entries[this.entryPos++];
    const accumulated: ColumnMap = {};

    for (const { batchIdx, rows } of entry.sections) {
      const rootStruct = rootStructOf(this.allBatches[batchIdx])!;
      mergeColumns(
        accumulated,
        this.layoutReader!.processSelectedRows(entry.index, rootStruct, rows),
      );
    }

    this._current = [entry.index, Arrow.tableFromArrays(accumulated as any)];
    return true;
  }

  async seek(index: bigint): Promise<boolean> {
    if (!this.initialized) await this.initialize();
    const currentIdx = this._current?.[0];
    if (currentIdx != null && index < currentIdx) {
      throw new Error(
        `Cannot seek backwards. Current: ${currentIdx}, requested: ${index}`,
      );
    }
    while (
      this.entryPos < this.entries.length &&
      this.entries[this.entryPos].index < index
    ) {
      this.entryPos++;
    }
    return (
      this.entryPos < this.entries.length &&
      this.entries[this.entryPos].index === index
    );
  }

  async next(): Promise<IteratorResult<[bigint, Arrow.Table]>> {
    const hasNext = await this.moveNextAsync();
    if (!hasNext) return { done: true, value: undefined as any };
    return { done: false, value: this._current! };
  }

  [Symbol.asyncIterator](): AsyncIterator<[bigint, Arrow.Table]> {
    return this;
  }
}
