# mzpeakts

A TypeScript implementation of the mzPeak file format. (*work-in-progress*)

This is an implementation of [mzPeak](https://github.com/HUPO-PSI/mzPeak) that reads data in the browser
and over the network. It provides (relatively) fast random access over data files, and handles quick access to metadata
on-demand. All processing happens within the client, no backend server needed.

This is not a wrapper around an implementation in another language. This uses pre-existing JavaScript and WASM libraries
to work with ZIP, Parquet, and Arrow, with all the gory details of mzPeak implemented directly in TypeScript. This work
would not have been possible without:

1. `parquet-wasm`: <a href="https://github.com/kylebarron/parquet-wasm">https://github.com/kylebarron/parquet-wasm</a>
2. `apache-arrow`: <a href="https://arrow.apache.org/js/current/">https://arrow.apache.org/js/current/</a>
3. `zip.js`: <a href="https://github.com/gildas-lormeau/zip.js/">https://github.com/gildas-lormeau/zip.js/</a>

## Demo

Try out the [demo](https://hupo-psi.github.io/mzpeakts/app.html) application for reading mzPeak files and visualizing their contents

## Library Usage

Assuming you are running this in a web browser with a static asset hosted `/static/small.mzpeak`

```ts
import { MzPeakReader, Spectrum } from "mzpeakts"

const reader = await MzPeakReader.fromUrl("/static/small.mzpeak")

const spec = await reader.getSpectrum(0)
console.log(spec.id, spec.index, spec.dataArrays)

const ms2Spec = await reader.getSpectrum(3)
console.log(spec.id, spec.index, spec.dataArrays, spec.precursors, spec.selectedIons)
```

## Status

- [x] Reading
  - [x] Array indices
  - [x] File-level metadata
  - [x] Spectrum metadata
  - [x] Chromatogram metadata
  - [x] Spectrum data arrays
    - [x] Point Layout
    - [x] Chunked Layout
      - [x] Basic encoding
      - [x] Delta encoding
      - [ ] Numpress and opaque chunk transforms
  - [x] Chromatogram data arrays
  - [x] Spectrum peak arrays
  - [x] Auxiliary arrays
  - [x] ZIP archive storage
  - [ ] Directory storage
