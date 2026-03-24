import { expect, test } from "vitest";
import * as fs from "fs"
import { ZipStorage } from "../src/store"
import { BlobReader, ZipReader } from "@zip.js/zip.js"

test("test launches", async () => {
    const blob = await fs.openAsBlob("static/small.mzpeak")

    const handle = new BlobReader(blob)
    if (handle.init) {
        console.log("Initializing")
        await handle.init()
    }
    console.log(await handle.readUint8Array(0, 30))
    const reader = new ZipStorage(handle);

})