# mzpeakts

A TypeScript implementation of the mzPeak file format. (*work-in-progress*)

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
  - [ ] Auxiliary arrays
  - [x] ZIP archive storage
  - [ ] Directory storage
