import * as Arrow from "apache-arrow";


export function linearSearch<T extends Arrow.DataType>(
  array: Arrow.Vector<T>,
  value: T["TValue"],
  start: number,
  end: number,
) {
    for(let i = start; i < end; i++) {
        if (array.get(i) == value) {
            return i
        }
    }
    return null
};


export function binarySearch<T extends Arrow.DataType>(array: Arrow.Vector<T>, value: T["TValue"]) : number {
    let lo = 0;
    let hi = array.length - 1;
    while (lo <= hi) {
        let mid = (lo + Math.floor(hi - lo) / 2)
        let val = array.get(mid)
        if (val == null) {
            const top = linearSearch(array, value, mid, hi);
            if (top !== null) return top;
            const bottom = linearSearch(array, value, lo, hi);
            if (bottom !== null) return bottom;
            else {
                return 0
            }
        };
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

export function binarySearchAll<T extends Arrow.DataType>(array: Arrow.Vector<T>, value: T["TValue"]) {
    const indexOf = binarySearch(array, value)

    if (array.get(indexOf) != value) {
        return null
    }

    const n = array.length - 1

    let lo = indexOf;
    while (lo > 0) {
        let val = array.get(lo - 1)
        if (val == value) {
            --lo;
        } else {
            break
        }
    }
    let hi = indexOf;
    while (hi < n) {
        let val = array.get(hi + 1)
        if (val == value) {
            hi++
        } else {
            break
        }
    }
    if (hi < n) {
        if (array.get(hi) == value) {
            ++hi
        }
    }
    return [lo, hi]
}