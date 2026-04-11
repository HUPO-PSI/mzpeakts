import { useState, useRef } from "react";

import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogTitle from "@mui/material/DialogTitle";
import TextField from "@mui/material/TextField";
import InputAdornment from "@mui/material/InputAdornment";
import FindInPageIcon from "@mui/icons-material/FindInPage";
import CheckCircleOutlineIcon from "@mui/icons-material/CheckCircleOutline";
import ErrorOutlineIcon from "@mui/icons-material/ErrorOutline";
import { useSpectrumViewer } from "./util";

type XICValidationState =
  | { status: "valid" }
  | { status: "error"; message: string };

interface XICModalProps {
  onLoad: (spec: XICTarget) => void;
}

class XICInputValue {
  _value: string;
  validation: XICValidationState;
  missingValue: number | undefined;
  constructor(value: string, missingValue?: number) {
    this._value = "";
    this.validation = validateNumber(value);
    this.value = value;
    this.missingValue = missingValue;
  }

  get value() {
    return this._value;
  }

  set value(value) {
    this._value = value;
    this.validate();
  }

  withValue(value: string) {
    return new XICInputValue(value, this.missingValue);
  }

  validate() {
    this.validation = validateNumber(this.value);
  }

  parse() {
    return this.value ? parseFloat(this.value) : this.missingValue || 0;
  }
}

export type XICTarget = {
  startTime: number;
  endTime: number;
  startMz: number;
  endMz: number;
};

const endAdornment = (v: XICInputValue) => {
  if (v.validation.status === "valid")
    return <CheckCircleOutlineIcon color="success" />;
  if (v.validation.status === "error")
    return <ErrorOutlineIcon color="error" />;
  return null;
};

const validateNumber = (val: string): XICValidationState => {
  if (val === "") return { status: "valid" };
  const parsed = parseFloat(val);
  if (isNaN(parsed)) return { status: "error", message: "Not a number" };
  if (parsed < 0) return { status: "error", message: "number is negative" };
  return { status: "valid" };
};

export function XICExtractDialog({ onLoad }: XICModalProps) {
  const [open, setOpen] = useState(false);
  const [startTime, setStartTime] = useState(new XICInputValue("", 0));
  const [endTime, setEndTime] = useState(new XICInputValue("", Infinity));
  const [startMz, setStartMz] = useState(new XICInputValue("", 0));
  const [endMz, setEndMz] = useState(new XICInputValue("", Infinity));

  const state = useSpectrumViewer();

  const debounceTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const validationGeneration = useRef(0);

  const handleClose = () => {
    if (debounceTimer.current) clearTimeout(debounceTimer.current);
    validationGeneration.current++;
    setOpen(false);
  };

  const prepareState = (): XICTarget => {
    return {
      startTime: startTime.parse(),
      endTime: endTime.parse(),
      startMz: startMz.parse(),
      endMz: endMz.parse(),
    };
  };

  const handleLoad = () => {
    onLoad(prepareState());
    handleClose();
  };
  return (
    <>
      <Button
        variant="contained"
        startIcon={<FindInPageIcon />}
        onClick={() => setOpen(true)}
        style={{ marginRight: "1em" }}
        disabled={state.mzReader ? false : true}
      >
        Extract XIC
      </Button>
      <Dialog open={open} onClose={handleClose} fullWidth maxWidth="sm">
        <DialogTitle>Extract XIC</DialogTitle>
        <DialogContent>
          <TextField
            autoFocus
            fullWidth
            label="Start time"
            placeholder="0..."
            value={startTime.value}
            onChange={(e) => {
              setStartTime(startTime.withValue(e.target.value));
            }}
            type="number"
            error={startTime.validation.status === "error"}
            helperText="The time to start extracting a trace from"
            margin="normal"
            slotProps={{
              input: {
                endAdornment: endAdornment(startTime) ? (
                  <InputAdornment position="end">
                    {endAdornment(startTime)}
                  </InputAdornment>
                ) : null,
              },
            }}
          />
          <TextField
            autoFocus
            fullWidth
            label="End time"
            placeholder="...end of the line"
            value={endTime.value}
            onChange={(e) => {
              setEndTime(endTime.withValue(e.target.value));
            }}
            type="number"
            helperText="The time to end extracting a trace at"
            error={endTime.validation.status === "error"}
            margin="normal"
            slotProps={{
              input: {
                endAdornment: endAdornment(endTime) ? (
                  <InputAdornment position="end">
                    {endAdornment(endTime)}
                  </InputAdornment>
                ) : null,
              },
            }}
          />
          <TextField
            autoFocus
            fullWidth
            label="Start m/z"
            placeholder="0..."
            value={startMz.value}
            onChange={(e) => {
              setStartMz(startMz.withValue(e.target.value));
            }}
            type="number"
            error={startMz.validation.status === "error"}
            helperText="The m/z to start extracting a trace from"
            margin="normal"
            slotProps={{
              input: {
                endAdornment: endAdornment(startMz) ? (
                  <InputAdornment position="end">
                    {endAdornment(startMz)}
                  </InputAdornment>
                ) : null,
              },
            }}
          />
          <TextField
            autoFocus
            fullWidth
            label="End m/z"
            placeholder="...end of the line"
            value={endMz.value}
            onChange={(e) => {
              setEndMz(endMz.withValue(e.target.value));
            }}
            type="number"
            error={endMz.validation.status === "error"}
            helperText="The m/z to end extracting a trace from"
            margin="normal"
            slotProps={{
              input: {
                endAdornment: endAdornment(endMz) ? (
                  <InputAdornment position="end">
                    {endAdornment(endMz)}
                  </InputAdornment>
                ) : null,
              },
            }}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={handleClose}>Cancel</Button>
          <Button variant="contained" onClick={handleLoad}>
            Load
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
