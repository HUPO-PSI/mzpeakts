import * as Arrow from "apache-arrow";


export function binarySearch<T extends Arrow.DataType>(array: Arrow.Vector<T>, value: T["TValue"]) : number {
    let lo = 0;
    let hi = array.length - 1;
    while (lo <= hi) {
        let mid = (lo + Math.floor(hi - lo) / 2)
        let val = array.get(mid)
        if (val == null) throw new Error("Binary search on a null-containing array");
        if (val < value) {
            lo = mid
        } else if (val > value) {
            hi = mid
        } else {
            return mid
        }
    }
    return 0
}