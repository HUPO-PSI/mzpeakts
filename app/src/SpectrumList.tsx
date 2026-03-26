import { forwardRef, Fragment, useState } from "react";
import { MZPeakReader, Param, Spectrum } from "mzpeakts";
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

export interface RowContext {
  clickHandler: Function;
}

export const VirtuosoTableComponents: TableComponents<Spectrum, RowContext> = {
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

export interface Column {
  name: string;
  format?: Function | undefined;
  numeric: boolean;
  width?: number | undefined;
  getter: Function;
  class?: string | undefined;
  eager: boolean
}

export const columnDefs: Column[] = [
  {
    name: "Index",
    numeric: true,
    getter: (spectrum: Spectrum) => {
      return spectrum.index.toString();
    },
    eager: true,
  },
  {
    name: "Native ID",
    numeric: false,
    getter: (spectrum: Spectrum) => spectrum.id,
    width: 350,
    class: "native-id-column",
    eager: false,
  },
  {
    name: "Time",
    numeric: true,
    format: (x: number) => x.toFixed(3),
    getter: (spectrum: Spectrum) => spectrum.time,
    eager: true,
  },
  {
    name: "Base Peak m/z",
    numeric: true,
    format: (x: number) => x.toFixed(2),
    getter: (spectrum: Spectrum) => {
      return spectrum.getParamByAccession("MS:1000504")?.value;
    },
    eager: false,
  },
  {
    name: "Base Peak Int.",
    numeric: true,
    format: (x: number) => x.toExponential(2),
    getter: (spectrum: Spectrum) =>
      spectrum.getParamByAccession("MS:1000505")?.value,
    eager: false,
  },
  {
    name: "MS Level",
    numeric: true,
    getter: (spectrum: Spectrum) => spectrum.msLevel,
    eager: true,
  },
  {
    name: "Prec. m/z",
    numeric: true,
    format: (x: number) => x.toFixed(3),
    getter: (spectrum: Spectrum) => {
      return spectrum.selectedIons.length ? spectrum.selectedIons[0].mz : null;
    },
    eager: true,
  },
  {
    name: "Prec. z",
    width: 40,
    numeric: true,
    getter: (spectrum: Spectrum) =>
      spectrum.selectedIons.length
        ? spectrum.selectedIons[0].chargeState
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

export function rowContent(
  _index: number,
  row: Spectrum,
  currentSpectrumID: string | undefined,
  isScrolling: boolean = false,
) {
  const isCurrentSpectrum = row.id == currentSpectrumID;
  return (
    <Fragment>
      {columnDefs.map((column) => {
        let value = isScrolling && !column.eager ? null : column.getter(row);
        if (column.format && value !== undefined && value !== null) {
          value = column.format(value);
        }
        return (
          <TableCell
            key={column.name}
            align={"center"}
            className={[
              isCurrentSpectrum ? "current-spectrum" : "",
              column.class ? column.class : "",
            ].join(" ")}
          >
            {value}
          </TableCell>
        );
      })}
    </Fragment>
  );
}

export function VirtualizedTable() {
  const viewerDispatch = useSpectrumViewerDispatch();
  const viewerState = useSpectrumViewer();
  const [isScrolling, setIsScrolling] = useState(false);
  const mzReader = viewerState.mzReader;
  const onClick = async (index: number) => {
    if (mzReader) {
      const spectrum = await mzReader.getSpectrum(index);
      viewerDispatch({
        type: ViewerActionType.CurrentSpectrumIdx,
        value: index,
        spectrum: spectrum,
      });
    }
  };



  const isMobile = useMediaQuery("(max-width:500px)");

  return (
    <Paper
      style={{
        height: 400,
        minWidth: 1000,
        overflowY: "hidden",
        overflowX: "hidden",
        marginLeft: isMobile ? "10em" : 0,
      }}
    >
      <TableVirtuoso
        totalCount={mzReader ? mzReader.length : 0}
        isScrolling={setIsScrolling}
        itemContent={(index: number) => {
          // mzReader?.setDataLoading(false);
          const reader = mzReader as MZPeakReader<any>;
          const metaReader = reader.spectrumMetadata;
          if (metaReader == null)
            throw new Error("Cannot handle missing spectra");

          return rowContent(
            index,
            metaReader.get(index),
            viewerState.currentSpectrumID,
            isScrolling,
          );
        }}
        context={{ clickHandler: onClick }}
        components={VirtuosoTableComponents}
        fixedHeaderContent={fixedHeaderContent}
      />
    </Paper>
  );
}

export function SpectrumList() {
  return VirtualizedTable();
}
