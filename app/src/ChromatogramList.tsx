import { forwardRef, Fragment, } from "react";
import { ChromatogramMetadata, MzPeakReader, Chromatogram } from "mzpeakts";
import "./SpectrumList.css";

import {
  TableContainer,
  Table,
  TableHead,
  TableBody,
  TableRow,
  TableCell,
  Paper,
} from "@mui/material";
import useMediaQuery from "@mui/material/useMediaQuery";
import { TableVirtuoso, TableComponents } from "react-virtuoso";
import {
  useSpectrumViewerDispatch,
  useSpectrumViewer,
  ViewerActionType,
} from "./util";
import React from "react";

interface RowContext {
  clickHandler: Function;
}

const VirtuosoTableComponents: TableComponents<Chromatogram, RowContext> = {
  Scroller: forwardRef<HTMLDivElement>((props, ref) => (
    <TableContainer component={Paper} {...props} ref={ref} />
  )),
  Table: (props) => (
    <Table
      {...props}
      size={"small"}
      sx={{
        borderCollapse: "separate",
        tableLayout: "fixed",
        minWidth: "100%",
      }}
    />
  ),
  TableHead: forwardRef<HTMLTableSectionElement>((props, ref) => (
    <TableHead {...props} ref={ref} />
  )),
  TableRow: (props) => {
    const clickHandler = props.context?.clickHandler;
    const row = (
      <TableRow
        {...props}
        onClick={(_) =>
          clickHandler ? clickHandler(props["data-index"]) : undefined
        }
      />
    );
    return row;
  },
  TableBody: forwardRef<HTMLTableSectionElement>((props, ref) => (
    <TableBody {...props} ref={ref} />
  )),
};

interface Column {
  name: string;
  format?: Function | undefined;
  numeric: boolean;
  width?: number | undefined;
  getter: Function;
  class?: string | undefined;
  eager: boolean
}

const columnDefs: Column[] = [
  {
    name: "Index",
    numeric: true,
    getter: (spectrum: Chromatogram) => {
      return spectrum.index.toString();
    },
    eager: true,
  },
  {
    name: "Native ID",
    numeric: false,
    getter: (spectrum: Chromatogram) => spectrum.id,
    width: 350,
    class: "native-id-column",
    eager: false,
  },
  // {
  //   name: "Base Peak m/z",
  //   numeric: true,
  //   format: (x: number) => x.toFixed(2),
  //   getter: (spectrum: Spectrum) => {
  //     return spectrum.getParamByAccession("MS:1000504")?.value;
  //   },
  //   eager: false,
  // },
  // {
  //   name: "Base Peak Int.",
  //   numeric: true,
  //   format: (x: number) => x.toExponential(2),
  //   getter: (spectrum: Spectrum) =>
  //     spectrum.getParamByAccession("MS:1000505")?.value,
  //   eager: false,
  // },
  {
    name: "Prec. m/z",
    numeric: true,
    format: (x: number) => x.toFixed(3),
    getter: (chromatogram: Chromatogram) => {
      return chromatogram.selectedIons.length ? chromatogram.selectedIons[0].mz : null;
    },
    eager: true,
  },
  {
    name: "Prec. z",
    numeric: true,
    getter: (chromatogram: Chromatogram) =>
      chromatogram.selectedIons.length
        ? chromatogram.selectedIons[0].chargeState
        : null,
    eager: true,
  },
];

export function fixedHeaderContent() {
  return (
    <TableRow>
      {columnDefs.map((column) => {
        const style: React.CSSProperties = {};
        if (column.width) {
          style["width"] = column.width;
        }
        return (
          <TableCell
            key={column.name}
            padding="none"
            variant="head"
            align={"center"}
            style={style}
            sx={{ backgroundColor: "background.paper" }}
          >
            {column.name}
          </TableCell>
        );
      })}
    </TableRow>
  );
}

export function rowContentBasic(
  index: number,
  handle: ChromatogramMetadata,
  currentSpectrumID: string | undefined,
) {
  const row = handle.get(index)
  const isCurrentSpectrum = row.id == currentSpectrumID;
  const selIon = row.selectedIons.length ? row.selectedIons[0] : null;
  const style: React.CSSProperties = { padding: "3px", textAlign: "center" };
  const className = isCurrentSpectrum ? "current-spectrum" : "";
  return (
    <Fragment key={index}>
      <TableCell key="index" className={className}>
        {row.index.toString()}
      </TableCell>
      <TableCell
        key="id"
        className={
          isCurrentSpectrum
            ? "native-id-column current-spectrum"
            : "native-id-column"
        }
        style={{ width: 350 }}
      >
        {row.id}
      </TableCell>
      <TableCell key="precursor-mz" style={style} className={className}>
        {selIon?.mz?.toFixed(3)}
      </TableCell>
      <TableCell key="precursor-z" style={style} className={className}>
        {selIon?.chargeState ?? ""}
      </TableCell>
    </Fragment>
  );
}


function rowContentBasicProp(props: {
  index: number,
  handle: ChromatogramMetadata,
  currentSpectrumID: string | undefined,
}) {
  return rowContentBasic(props.index, props.handle, props.currentSpectrumID)
}


const RowContentMemo = React.memo(
  rowContentBasicProp,
  (prevProps, nextProps) => {
    return prevProps.index == nextProps.index && prevProps.currentSpectrumID == nextProps.currentSpectrumID;
  },
);


function VirtualizedTable() {
  const viewerDispatch = useSpectrumViewerDispatch();
  const viewerState = useSpectrumViewer();
  const mzReader = viewerState.mzReader;
  const onClick = async (index: number) => {
    if (mzReader) {
        const chromatogram = await mzReader.getChromatogram(index);
        console.log("Dispatching chromatogram", index, chromatogram)
      viewerDispatch({
        type: ViewerActionType.CurrentChromatogramIdx,
        value: index,
        chromatogram,
      });
    }
  };

  const isMobile = useMediaQuery("(max-width:500px)");

  return (
    <>
      <Paper
        style={{
          height: 300,
          minWidth: 1000,
          overflowY: "hidden",
          overflowX: "hidden",
          marginLeft: isMobile ? "10em" : 0,
        }}
      >
        <TableVirtuoso
          totalCount={mzReader ? mzReader.chromatogramMetadata?.length : 0}
          itemContent={(index: number) => {
            const reader = mzReader as MzPeakReader<any>;
            const metaReader = reader.chromatogramMetadata;
            if (metaReader == null)
              throw new Error("Cannot handle missing chromatograms");

            return (
              <RowContentMemo
                index={index}
                handle={metaReader}
                currentSpectrumID={viewerState.currentSpectrumID}
              />
            );
          }}
          context={{ clickHandler: onClick }}
          components={VirtuosoTableComponents}
          fixedHeaderContent={fixedHeaderContent}
          style={{ overflow: "scroll" }}
        />
      </Paper>
    </>
  );
}

export function ChromatogramList() {
  return VirtualizedTable();
}
