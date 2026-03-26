# Demo Application: Spectrum Viewer

This application runs entirely in the web browser, doing most of the processing with the
`mzdata-wasm` library. Pick an `.mzML` or `.mgf` file and the application will lazily load
spectra to view.

The application includes a set of signal processing functions controlled from the sidebar.

1. __Reprofiling__ - Convert centroid spectra into profile spectra ease of viewing and allows denoising.
2. __Denoising__ - Apply an iterative denoising and baseline reduction.
3. __Deconvolution__ - Apply a pre-configured `mzdeisotope` charge state deconvolution treatment to identify isotopic patterns.

