import * as Arrow from "apache-arrow";
import * as ArrowFFI from "arrow-js-ffi";
import { ParquetFile, wasmMemory, ReaderOptions } from "parquet-wasm";

import {
  ArrayIndex,
  ArrayIndexEntry,
  BufferContext,
  BufferFormat,
  bufferContextIndexName,
  bufferContextName,
} from "./array_index";
import { FloatArray, IntArray } from "apache-arrow/type";
import { bigIntToNumber } from "apache-arrow/util/bigint";

export type DataArrays = Record<string, FloatArray | IntArray | string[]>;

export function packTableIntoDataArrays(table: Arrow.Table): DataArrays {
  const dataArrays: DataArrays = {};
  for (let i = 1; i < table.schema.fields.length; i++) {
    const colName = table.schema.fields[i].name;
    dataArrays[colName] = table.getChildAt(i)?.toArray();
  }
  return dataArrays;
}

export class SpacingInterpolationModel {
  coefficients: number[];

  constructor(coefficients: number[]) {
    this.coefficients = coefficients;
  }

  predict(value: number) {
    let acc = 0.0;
    for (let i = 0; i < this.coefficients.length; i++) {
      acc += Math.pow(value, i) * this.coefficients[i];
    }
    return acc;
  }

  static fromArrow(value: Arrow.Vector<Arrow.Float64>) {
    return new this(Array.from(value.toArray()));
  }
}

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
  columns?: string[],
): Promise<Arrow.RecordBatch[]> {
  const options: ReaderOptions = rowGroups != null ? { rowGroups } : {};
  if (columns) options.columns = columns;
  const tab = await handle.read(options);
  const mem = wasmMemory();
  const ffi = tab.intoFFI();
  const arrowTab = ArrowFFI.parseTable(
    mem.buffer,
    ffi.arrayAddrs(),
    ffi.schemaAddr(),
  );
  ffi.free();
  return arrowTab.batches;
}

async function* streamArrowBatches(
  handle: ParquetFile,
  rowGroups?: number[],
  columns?: string[],
  options_?: ReaderOptions,
) {
  const options = options_ ?? {};
  options.rowGroups = rowGroups;
  if (columns) options.columns = columns;
  const tabStream = (await handle.stream(options)).values();
  const mem = wasmMemory();
  while (true) {
    let batch = await tabStream.next();
    if (batch.done) break;
    let wBatch = batch.value;
    if (wBatch) {
      let ffi = wBatch.intoFFI();
      let arrowBat = ArrowFFI.parseRecordBatch(
        mem.buffer,
        ffi.arrayAddr(),
        ffi.schemaAddr(),
      );
      ffi.free();

      yield arrowBat;
    }
  }
}

function combineVectors<T extends Arrow.DataType>(
  arrays: Arrow.Vector<T>[],
): Arrow.Vector<T> {
  if (arrays.length == 1) return arrays[0];
  const acc = Arrow.makeBuilder({ type: arrays[0].type, nullValues: [null] });
  for (let i = 0; i < arrays.length; i++) {
    const chunk = arrays[i];
    for (let j = 0; j < chunk.length; j++) {
      acc.append(chunk.get(j));
    }
  }
  return acc.finish().toVector();
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

function decodeNoCompression(
  startValue: number,
  values: Arrow.Vector<Arrow.Float>,
): Arrow.Vector<Arrow.Float> {
  const acc = Arrow.makeBuilder({ type: values.type, nullValues: [null] });
  acc.append(startValue);
  for (let val of values) {
    acc.append(val);
  }
  return acc.finish().toVector();
}

function decodeDelta(
  startValue: number,
  values: Arrow.Vector<Arrow.Float>,
): Arrow.Vector<Arrow.Float> {
  const acc = Arrow.makeBuilder({ type: values.type, nullValues: [null] });
  let last: number | null = startValue;
  if (!values.isValid(0)) {
    if (!values.isValid(1)) {
      acc.append(last);
    }
    last = null;
  }
  for (let val of values) {
    if (val != null) {
      if (last != null) {
        last = val + last;
        acc.append(last);
      } else {
        acc.append(val);
        last = val;
      }
    } else {
      acc.append(val);
      last = val;
    }
  }
  const result = acc.finish().toVector();
  return result;
}

const nullToZero = <T extends Arrow.DataType>(array: Arrow.Vector<T>) => {
  for (let _i = 0; _i < array.length; _i++) {
    if (!array.isValid(_i)) {
      array.set(_i, 0);
    }
  }
  return array;
};

interface JsStatistics<T> {
  min_value: T | undefined;
  max_value: T | undefined;
  // Distinct count could be omitted in some cases
  distinct_count: number | undefined;
  null_count: number | undefined;

  // Whether or not the min or max values are exact, or truncated.
  is_max_value_exact: boolean;
  is_min_value_exact: boolean;
}

/**
 * Construct index ranges between pairs of masked values in `maskedVector`.
 *
 * The first and last index range will include the beginning and ending
 * of the array respectively, even if the mask does not start/end with a
 * `true` value.
 *
 * The resulting array contains [start, end) pairs (end is exclusive) of the
 * spans between two `true` values (or the termini of the array).
 *
 * Warning: can fail or produce incorrect output if there are runs of
 * `true` values longer than 2 in the mask.
 */
export function findMaskedPairs(
  maskedVector: Arrow.Vector,
): [number, number][] {
  const indices: number[] = [];
  for (let i = 0; i < maskedVector.length; i++) {
    if (!maskedVector.isValid(i)) indices.push(i);
  }
  if (indices.length === 0) {
    return [[0, maskedVector.length]];
  }
  const parts: number[] = [];
  if (indices[0] !== 0) parts.push(0);
  parts.push(...indices);
  if (indices[indices.length - 1] !== maskedVector.length - 1)
    parts.push(maskedVector.length - 1);
  const result: [number, number][] = [];
  for (let i = 0; i < parts.length; i += 2) {
    result.push([parts[i], parts[i + 1] + 1]);
  }
  return result;
}

function median(arr: number[]): number {
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[mid - 1] + sorted[mid]) / 2
    : sorted[mid];
}

/**
 * Find the 2nd median of the consecutive differences of `data`.
 *
 * This is a relatively crude spacing estimate for continuous profile data.
 *
 * @returns A tuple of [secondMedian, filteredDeltas] where filteredDeltas are
 * the diff values that are <= the first median.
 */
export function estimateMedianDelta(
  data: number[] | FloatArray,
): [number, number[]] {
  const deltas: number[] = [];
  for (let i = 1; i < data.length; i++) {
    deltas.push(data[i] - data[i - 1]);
  }
  const med1 = median(deltas);
  const deltasBelow = deltas.filter((d) => d <= med1);
  const med2 = median(deltasBelow);
  return [med2, deltasBelow];
}

function interpolateNulls(
  values: Arrow.Vector<Arrow.Float>,
  model: SpacingInterpolationModel,
): Arrow.Vector<Arrow.Float> {
  const pairIndices = findMaskedPairs(values);
  const chunks = [];
  for (let [start, end] of pairIndices) {
    const chunk = values.slice(start, end);
    const n = chunk.length;
    const nHasReal = n - chunk.nullCount;
    if (nHasReal == 1) {
      if (n == 2) {
        if (chunk.isValid(1)) {
          const v = chunk.get(1) ?? 0;
          chunk.set(0, v - model.predict(v));
        } else {
          const v = chunk.get(0) ?? 0;
          chunk.set(1, v + model.predict(v));
        }
      } else if (n == 3) {
        const v = chunk.get(1) ?? 0;
        chunk.set(0, v - model.predict(v));
        chunk.set(2, v + model.predict(v));
      } else {
        throw new Error(`Chunk ${start}-${end} is too short to interpolate!?`);
      }
    } else {
      const [dx, _] = estimateMedianDelta(chunk.toArray());
      chunk.set(0, (chunk.get(1) ?? 0) - dx);
      chunk.set(chunk.length - 1, (chunk.get(chunk.length - 2) ?? 0) + dx);
    }
    chunks.push(chunk);
  }
  return combineVectors(chunks);
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
    const value_ = BigInt(value);
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
    const index = BigInt(index_);
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
  ready: Promise<boolean> | boolean;

  constructor(
    context: BufferContext,
    arrayIndex: ArrayIndex,
    rowGroupIndex: RangeIndex,
    entrySpanIndex: RangeIndex,
    format: BufferFormat,
    spacingModels: Map<bigint, SpacingInterpolationModel> | null = null,
    ready: Promise<boolean> | boolean = false,
  ) {
    if (ready != true && ready != false) {
      ready = ready.then((value) => {
        this.ready = value;
        // console.log("Finished asynchronously loading indices")
        return value;
      });
    }
    this.context = context;
    this.arrayIndex = arrayIndex;
    this.rowGroupIndex = rowGroupIndex;
    this.entrySpanIndex = entrySpanIndex;
    this.format = format;
    this.spacingModels = spacingModels;
    this.ready = ready;
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
    else throw new Error(`Root schema prefix "${firstColPath}" not recognized`);

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

    const rowGroupBounds: GroupTagBounds[] = [];
    for (let i = 0; i < pqMeta.numRowGroups(); i++) {
      let rg = pqMeta.rowGroup(i);
      let idxCol = rg.column(0);
      let stats: JsStatistics<number> | null = idxCol.statistics();
      if (stats != null) {
        if (stats.min_value != undefined && stats.max_value != undefined) {
          rowGroupBounds.push(
            new GroupTagBounds(
              BigInt(i),
              BigInt(stats.min_value),
              BigInt(stats.max_value),
            ),
          );
        }
      }
    }

    const rowSpanBounds: GroupTagBounds[] = [];
    const processing = async () => {
      // Build RangeIndex and EntrySpanIndex by scanning index column per row group.
      // This can be time intensive, so let's put this task on the backburner and use the slower
      // row group-level scanning while we are waiting
      const idxColName = bufferContextIndexName(context);

      let offset = 0n;
      let lastIdx = 0n;
      let spanStart = 0n;
      let seenFirst = false;
      for (let i = 0; i < nRowGroups; i++) {
        const batches = await readArrowBatches(
          handle,
          [i],
          [`${arrayIndex.prefix}.${idxColName}`],
        );
        let rgMin: bigint | null = null;
        let rgMax: bigint | null = null;

        for (const batch of batches) {
          const rootStruct = rootStructOf(batch);
          if (rootStruct == null) {
            offset += BigInt(batch.numRows);
            continue;
          }
          const indexVec = rootStruct.getChild(
            idxColName,
          ) as Arrow.Vector<Arrow.Uint64> | null;
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
      }
      if (seenFirst) {
        rowSpanBounds.push(new GroupTagBounds(lastIdx, spanStart, offset - 1n));
      }
      // console.log("Parquet indices ready")
      return true;
    };

    return new DataArraysReaderMeta(
      context,
      arrayIndex,
      new RangeIndex(rowGroupBounds),
      new RangeIndex(rowSpanBounds),
      format,
      null,
      processing(),
    );
  }
}

// ---- Layout readers ----

type ColumnMap = Record<string, Arrow.Vector>;

const take = <T extends Arrow.DataType>(
  array: Arrow.Vector<T>,
  indices: number[],
) => {
  const acc = Arrow.makeBuilder({ type: array.type, nullValues: [null] });
  for (let i of indices) {
    acc.append(array.get(i));
  }
  return acc.finish().toVector();
};

const firstNotNull = <T extends Arrow.DataType>(
  array: Arrow.Vector<T>,
): T["TValue"] | null => {
  for (let i = 0; i < array.length; i++) {
    let v = array.get(i);
    if (v != null) return v;
  }
  return null;
};

const lastNotNull = <T extends Arrow.DataType>(
  array: Arrow.Vector<T>,
): T["TValue"] | null => {
  for (let i = array.length - 1; i >= 0; i--) {
    let v = array.get(i);
    if (v != null) return v;
  }
  return null;
};

export class BaseLayoutReader {
  public arrayIndex: ArrayIndex;
  protected batches: AsyncIterableIterator<Arrow.RecordBatch>;
  protected spacingModels: Map<bigint, SpacingInterpolationModel> | undefined;

  constructor(
    batches: AsyncIterableIterator<Arrow.RecordBatch>,
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
      if (selectedRows.length == vec.length) {
        result[field.name] = vec;
      } else {
        result[field.name] = take(vec, selectedRows);
      }
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
    startFrom: bigint | null,
    endAt: bigint | null,
  ): Promise<Arrow.Table> {
    let rowCountRead = 0n;
    const accumulated: Record<string, Arrow.Vector[]> = {};

    let nBats = 0;
    for await (const batch of this.batches) {
      const rootStruct = rootStructOf(batch);
      if (rootStruct == null) {
        rowCountRead += BigInt(batch.numRows);
        continue;
      }

      const batchSize = BigInt(rootStruct.length);
      if (startFrom != null) {
        if (rowCountRead + batchSize < startFrom) {
          rowCountRead += batchSize;
          continue;
        }
      } else {
        const firstIdxOf = firstNotNull(indexVectorOf(rootStruct));
        if (firstIdxOf != null && firstIdxOf > entryIndex) break;
      }

      if (endAt != null) {
        if (rowCountRead >= endAt) break;
      } else {
        const lastIdxOf = lastNotNull(indexVectorOf(rootStruct));
        if (lastIdxOf != null && lastIdxOf < entryIndex) {
          continue;
        }
      }

      const entries = this.processRows(entryIndex, rootStruct);
      nBats += 1;
      for (let [k, v] of Object.entries(entries)) {
        if (accumulated[k] == undefined) {
          accumulated[k] = [v];
        } else {
          accumulated[k].push(v);
        }
      }
      rowCountRead += batchSize;
    }
    const final: Record<string, Arrow.Vector> = {};
    if (nBats == 1) {
      for (let [k, v] of Object.entries(accumulated)) {
        final[this.arrayIndex.fieldToName.get(k) ?? k] = v[0];
      }
    } else {
      for (let [k, v] of Object.entries(accumulated)) {
        final[this.arrayIndex.fieldToName.get(k) ?? k] = combineVectors(v);
      }
    }

    return Arrow.tableFromArrays(final as any);
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
      const fieldName = entry.fieldName;
      if (!(fieldName in base)) continue;

      if (entry.transform === NULL_ZERO_CURIE) {
        base[fieldName] = nullToZero(base[fieldName]);
      } else if (
        entry.transform === NULL_INTERPOLATE_CURIE &&
        this.spacingModels?.has(entryIndex)
      ) {
        base[fieldName] = interpolateNulls(
          base[fieldName],
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
    batches: AsyncIterableIterator<Arrow.RecordBatch>,
    arrayIndex: ArrayIndex,
    spacingModels?: Map<bigint, SpacingInterpolationModel>,
  ) {
    super(batches, arrayIndex, spacingModels);
    this.configureIndices();
  }

  private configureIndices() {
    for (const entry of this.arrayIndex.entries) {
      const fieldName = entry.fieldName;
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
    if (this.mainAxisEntry == null) throw new Error("Main axis cannot be null");

    const chunkStartVec = rootStruct.getChild(this.chunkStartFieldName)!;
    const chunkEncodingVec = rootStruct.getChild(
      this.chunkEncodingFieldName,
    ) as Arrow.Vector<Arrow.Utf8>;
    const chunkValuesVec = rootStruct.getChild(
      this.chunkValuesFieldName,
    ) as Arrow.Vector<Arrow.List<Arrow.DataType>>;

    const indexColName = bufferContextIndexName(this.mainAxisEntry.context);
    const mainAxisName = this.chunkValuesFieldName;

    const resultIndex: Arrow.Uint64Builder = Arrow.makeBuilder({
      type: new Arrow.Uint64(),
      nullValues: [null],
    });
    const resultMainAxis: Arrow.Vector<Arrow.Float>[] = [];
    const resultSecondary: Record<string, Arrow.Vector[]> = {};
    for (const { name } of this.secondaryFields) resultSecondary[name] = [];

    for (const rowIdx of selectedRows) {
      const startValue = Number(chunkStartVec.get(rowIdx) ?? 0);
      const encoding = chunkEncodingVec.get(rowIdx) ?? "";
      const chunkValues = chunkValuesVec.get(
        rowIdx,
      ) as Arrow.Vector<Arrow.Float> | null;
      if (chunkValues == null)
        throw new Error(
          `Chunk values cannot be null, but ${rowIdx} with start value ${startValue} for ${entryIndex} was`,
        );
      let decoded: Arrow.Vector<Arrow.Float>;
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
      resultMainAxis.push(decoded);
      for (let _i = 0; _i < decoded.length; _i++)
        resultIndex.append(entryIndex);

      for (const { name, entry } of this.secondaryFields) {
        const secVec = rootStruct.getChild(name) as Arrow.Vector<
          Arrow.List<Arrow.DataType>
        >;
        const secValues = secVec.get(rowIdx);
        if (secValues == null) continue;

        if (entry.transform === NULL_ZERO_CURIE) {
          for (let _i = 0; _i < secValues.length; _i++) {
            if (!secValues.isValid(_i)) {
              secValues.set(_i, 0);
            }
          }
          resultSecondary[name].push(secValues);
        } else if (entry.transform === NUMPRESS_SLOF_CURIE) {
          throw new Error(
            `Numpress decoding not implemented for secondary axis (transform: ${entry.transform})`,
          );
        } else {
          resultSecondary[name].push(secValues);
        }
      }
    }
    let mainAxisCombined = combineVectors(resultMainAxis);
    const spacingModel = this.spacingModels?.get(entryIndex);
    if (spacingModel) {
      mainAxisCombined = interpolateNulls(mainAxisCombined, spacingModel);
    }
    const result: ColumnMap = {
      [indexColName]: resultIndex.finish().toVector(),
      [mainAxisName]: mainAxisCombined,
    } as ColumnMap;
    for (const [name, values] of Object.entries(resultSecondary)) {
      result[name] = combineVectors(values);
    }
    return result;
  }
}

// ---- DataArraysReader ----

export class DataArraysReader {
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
    return new this(handle, meta);
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

  makeLayoutReader(
    batches: AsyncIterableIterator<Arrow.RecordBatch>,
  ): BaseLayoutReader {
    const models = this.spacingModels ?? undefined;
    if (this.metadata.format === BufferFormat.Point) {
      return new PointLayoutReader(batches, this.metadata.arrayIndex, models);
    } else if (this.metadata.format === BufferFormat.ChunkValues) {
      return new ChunkLayoutReader(batches, this.metadata.arrayIndex, models);
    }
    throw new Error("Data layout not recognized");
  }

  async checkIndices() {
    if (this.metadata.ready != true) {
      this.metadata.ready = await this.metadata.ready;
      if (!this.metadata.ready) {
        throw new Error("Parquet indices failed to load!");
      }
    }
  }

  indicesReady() {
    return this.metadata.ready == true;
  }

  async get(key_: bigint | number): Promise<Arrow.Table | null> {
    const key = BigInt(key_);
    if (this.indicesReady()) {
      await this.checkIndices();
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
      const batches = streamArrowBatches(
        this.handle,
        rowGroups.map(Number),
        undefined,
        {
          offset: bigIntToNumber(rowSpan.start - offset) - 1,
          limit: bigIntToNumber(rowSpan.end - rowSpan.start) + 1,
        },
      );
      const layoutReader = this.makeLayoutReader(batches);
      return layoutReader.readRowsOf(key, 0n, rowSpan.end - rowSpan.start);
    } else {
      const rowGroups = this.rowGroupIndex.keysFor(key);
      if (rowGroups.length === 0) return null;
      const batches = streamArrowBatches(
        this.handle,
        rowGroups.map(Number),
        undefined,
      );
      const layoutReader = this.makeLayoutReader(batches);
      return layoutReader.readRowsOf(key, null, null);
    }
  }

  enumerate(): DataArraysIter {
    return new DataArraysIter(this, streamArrowBatches(this.handle));
  }

  [Symbol.asyncIterator](): AsyncIterator<
    [bigint, Arrow.Table | ColumnMap]
  > {
    return this.enumerate();
  }
}

function vectorEquals<T extends Arrow.DataType>(
  array: Arrow.Vector<T>,
  value: T["TValue"],
) {
  const acc: boolean[] = [];
  for (let v of array) {
    acc.push(v == value);
  }
  return acc;
}

function vectorWhere(mask: boolean[]) {
  const indices: number[] = [];
  for (let i = 0; i < mask.length; i++) {
    if (mask[i]) {
      indices.push(i);
    }
  }
  return indices;
}

// ---- DataArraysIter ----

export class DataArraysIter
  implements
    AsyncIterator<[bigint, Arrow.Table | ColumnMap]>,
    AsyncIterable<[bigint, Arrow.Table | ColumnMap]>
{
  private reader: DataArraysReader;
  private batchStream: AsyncIterableIterator<Arrow.RecordBatch>;
  private layoutReader: BaseLayoutReader;
  private currentBatch: Arrow.Vector<Arrow.Struct> | null = null;
  private currentIndex: bigint | null = null;
  private _current: [bigint, Arrow.Vector<Arrow.Struct> | ColumnMap] | null =
    null;
  private initialized: boolean = false;

  constructor(
    reader: DataArraysReader,
    batchStream: AsyncIterableIterator<Arrow.RecordBatch>,
  ) {
    this.reader = reader;
    this.batchStream = batchStream;
    this.layoutReader = this.reader.makeLayoutReader(this.batchStream);
  }

  get current(): [bigint, Arrow.Vector<Arrow.Struct> | ColumnMap] {
    if (!this._current)
      throw new Error("Iterator not initialized or exhausted");
    return this._current;
  }

  private async readNextBatch(updateIndex: boolean = false) {
    this.currentBatch = null;
    let batchMsg = await this.batchStream.next();
    if (batchMsg.done || batchMsg.value == null) return false;
    const batch = batchMsg.value;
    const root = batch.getChildAt(0) as Arrow.Vector<Arrow.Struct>;
    if (root == null) return false;
    const idxCol = root.getChildAt(0) as Arrow.Vector<Arrow.Uint64>;
    const lowestIndex = idxCol
      .toArray()
      .reduce((prev, cur) => (prev < cur ? prev : cur));
    this.currentBatch = root;
    if (
      updateIndex &&
      ((this.currentIndex !== null && lowestIndex > this.currentIndex) ||
        this.currentIndex === null)
    ) {
      this.currentIndex = lowestIndex;
    }
    return true;
  }

  private async initialize() {
    if (this.initialized) return this.currentBatch != null;
    await this.readNextBatch(true);
    this.initialized = true;
    return this.currentBatch != null;
  }

  private batchHasCurrentIndex() {
    if (this.currentBatch == null || this.currentIndex == null) return false;
    const indexArr = this.currentBatch.getChildAt(
      0,
    ) as Arrow.Vector<Arrow.Uint64>;
    const mask = vectorEquals(indexArr, this.currentIndex);
    return mask.some((e) => e);
  }

  private async extractForCurrentIndex(): Promise<Arrow.Vector<Arrow.Struct> | null> {
    if (this.currentBatch == null || this.currentIndex == null) return null;
    const indexArr = this.currentBatch.getChildAt(
      0,
    ) as Arrow.Vector<Arrow.Uint64>;
    const mask = vectorEquals(indexArr, this.currentIndex);
    const indices = vectorWhere(mask);
    const lastPossibleRowIndex = this.currentBatch.length - 1;
    let n: number;
    let start: number;
    let chunk: Arrow.Vector<Arrow.Struct>;
    if (indices.length == 0) {
      n = this.currentBatch.length;
      start = 0;
      chunk = this.currentBatch.slice(0, 0);
    } else {
      start = indices[0];
      n = indices.length;
      chunk = this.currentBatch.slice(start, n);
    }

    if (
      n == this.currentBatch.length ||
      indices.includes(lastPossibleRowIndex)
    ) {
      if (await this.readNextBatch(false)) {
        if (this.batchHasCurrentIndex()) {
          const rest = await this.extractForCurrentIndex();
          if (rest) {
            chunk = chunk.concat(rest);
          }
        }
      }
    } else {
      this.currentBatch.slice(n, this.currentBatch.length);
    }
    return chunk;
  }

  async moveNextAsync(doProcess: boolean = true) {
    if (this.currentIndex == null) {
      if (!(await this.initialize())) return false;
    }
    if (this.currentBatch == null) return false;
    if (this.currentIndex == null) return false;
    let nextBatch = await this.extractForCurrentIndex();
    if (nextBatch == null) return false;
    let index = this.currentIndex;
    if (doProcess) {
      const nextBatchUnpacked = this.layoutReader.processRows(index, nextBatch);

      this._current = [index, nextBatchUnpacked];
    } else {
      this._current = [index, nextBatch];
    }
    ++this.currentIndex;
    return true;
  }

  async seek(index_: bigint): Promise<boolean> {
    const index = BigInt(index_);
    if (!this.initialized) await this.initialize();
    if (this.currentIndex == null) return false;

    const currentIdx = this._current?.[0];
    if (currentIdx != null && index < currentIdx) {
      throw new Error(
        `Cannot seek backwards. Current: ${currentIdx}, requested: ${index}`,
      );
    }
    while (this.currentIndex != index) {
      await this.moveNextAsync(false);
    }
    return true;
  }

  async next(): Promise<
    IteratorResult<[bigint, Arrow.Table | ColumnMap]>
  > {
    const hasNext = await this.moveNextAsync();
    if (!hasNext) return { done: true, value: undefined as any };
    const value = this._current
    if (value == null) throw new Error("Cannot be null")
    const final: ColumnMap = {};
    for (let [k, v] of Object.entries(value[1])) {
      final[this.layoutReader.arrayIndex.fieldToName.get(k) ?? k] = v;
    }
    const payload: [bigint, Arrow.Table] = [value[0], Arrow.tableFromArrays(final as any) as Arrow.Table];
    return { done: false, value: payload };
  }

  [Symbol.asyncIterator](): AsyncIterator<
    [bigint, Arrow.Table | ColumnMap]
  > {
    return this;
  }
}
