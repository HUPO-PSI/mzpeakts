import { describe, it, expect } from "vitest";
import { Param } from "../src/metadata";

// Regression: a spectrum/scan/precursor/selected-ion with no cvParams is valid — a scan may
// legitimately carry zero cvParams. Before the null-guard, `Param.fromArrow(null)` threw
// `Cannot read properties of undefined (reading 'getChild')`, which made `SpectrumMetadata.get()`
// throw for every spectrum whose scan (or other sub-record) lacked a `parameters` list.
describe("Param.fromArrow null-safety", () => {
  it("returns [] for a null parameters array (missing params list)", () => {
    expect(Param.fromArrow(null as never)).toEqual([]);
  });
  it("returns [] for an undefined parameters array", () => {
    expect(Param.fromArrow(undefined as never)).toEqual([]);
  });
});
