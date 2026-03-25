import { expect, test, beforeAll } from "vitest";
import * as fs from "fs"
import { MZPeakReader } from '../src/reader';



test("test launches", async () => {
    const blob = await fs.openAsBlob("static/small.mzpeak")
    const reader = await MZPeakReader.fromBlob(blob);
    expect.assert(reader.length == 48)
})


test("data facet reader", async () => {
    const blob = await fs.openAsBlob("static/small.chunked.mzpeak");
    const reader = await MZPeakReader.fromBlob(blob);
    const dataReader = await reader.spectrumData();
    expect.assert(dataReader != null)
    const response = await dataReader.get(0);
})