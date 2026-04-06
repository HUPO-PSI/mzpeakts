import { ChangeEvent, useState, useRef } from "react";

import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogTitle from "@mui/material/DialogTitle";
import TextField from "@mui/material/TextField";
import CircularProgress from "@mui/material/CircularProgress";
import InputAdornment from "@mui/material/InputAdornment";
import { styled } from "@mui/material/styles";
import FileOpenIcon from "@mui/icons-material/FileOpen";
import LinkIcon from "@mui/icons-material/Link";
import CheckCircleOutlineIcon from "@mui/icons-material/CheckCircleOutline";
import ErrorOutlineIcon from "@mui/icons-material/ErrorOutline";
import ErrorIcon from "@mui/icons-material/Error";
import { store } from "mzpeakts";

import { useSpectrumViewerDispatch, ViewerActionType } from "./util";
import { ZipStorage } from "mzpeakts/src/store";

type UrlValidationState =
  | { status: "idle" }
  | { status: "loading" }
  | { status: "valid" }
  | { status: "error"; message: string };

interface RemoteUrlModalProps {
  onLoad: (url: string) => void;
}

function RemoteUrlModal({ onLoad }: RemoteUrlModalProps) {
  const [open, setOpen] = useState(false);
  const [urlInput, setUrlInput] = useState("");
  const [validation, setValidation] = useState<UrlValidationState>({ status: "idle" });
  const debounceTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const validationGeneration = useRef(0);

  const handleClose = () => {
    if (debounceTimer.current) clearTimeout(debounceTimer.current);
    validationGeneration.current++;
    setOpen(false);
    setUrlInput("");
    setValidation({ status: "idle" });
  };

  const validateUrl = async (url: string) => {
    const generation = ++validationGeneration.current;
    try {
      await store.ZipStorage.fromUrl(url);
      if (generation === validationGeneration.current) {
        setValidation({ status: "valid" });
      }
    } catch (err: any) {
      if (generation === validationGeneration.current) {
        console.log(err)
        setValidation({
          status: "error",
          message: err?.message ?? "Invalid or unreachable URL",
        });
      }
    }
  };

  const handleUrlChange = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.currentTarget.value;
    setUrlInput(value);
    if (debounceTimer.current) clearTimeout(debounceTimer.current);
    if (!value) {
      setValidation({ status: "idle" });
      return;
    }
    setValidation({ status: "loading" });
    debounceTimer.current = setTimeout(() => validateUrl(value), 500);
  };

  const handleLoad = () => {
    onLoad(urlInput);
    handleClose();
  };

  const endAdornment = (() => {
    if (validation.status === "loading") return <CircularProgress size={20} />;
    if (validation.status === "valid") return <CheckCircleOutlineIcon color="success" />;
    if (validation.status === "error") return <ErrorOutlineIcon color="error" />;
    return null;
  })();

  const helperText = (() => {
    if (validation.status === "error") return validation.message;
    if (validation.status === "valid") return "URL is valid";
    return "\u00a0";
  })();

  return (
    <>
      <Button
        variant="contained"
        startIcon={<LinkIcon />}
        onClick={() => setOpen(true)}
        style={{ marginRight: "1em" }}
      >
        Remote URL
      </Button>
      <Dialog open={open} onClose={handleClose} fullWidth maxWidth="sm">
        <DialogTitle>Load Remote File</DialogTitle>
        <DialogContent>
          <TextField
            autoFocus
            fullWidth
            label="File URL"
            placeholder="https://example.com/file.mzpeak"
            value={urlInput}
            onChange={handleUrlChange}
            error={validation.status === "error"}
            helperText={helperText}
            margin="normal"
            slotProps={{
              input: {
                endAdornment: endAdornment ? (
                  <InputAdornment position="end">{endAdornment}</InputAdornment>
                ) : null,
              },
            }}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={handleClose}>Cancel</Button>
          <Button
            variant="contained"
            onClick={handleLoad}
            disabled={validation.status !== "valid"}
          >
            Load
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}

const VisuallyHiddenInput = styled("input")({
  clip: "rect(0 0 0 0)",
  clipPath: "inset(50%)",
  height: 1,
  overflow: "hidden",
  position: "absolute",
  bottom: 0,
  left: 0,
  whiteSpace: "nowrap",
  width: 1,
});

interface DataFileChooserProps {
  dataFile: File | null;
  setDataFile: Function;
  setDataUrl?: (url: string) => void;
}

export function DataFileChooser({
  dataFile,
  setDataFile,
  setDataUrl,
}: DataFileChooserProps) {
  const dispatch = useSpectrumViewerDispatch();
  const onChangeHandler = async (e: ChangeEvent<HTMLInputElement>) => {
    const target = e.currentTarget;
    if (target?.files && target.files.length > 0) {
      console.log(`Updating data file`, dataFile, target.files);
      dispatch({
        text: "Loading data file",
        icon: <CircularProgress size={16} color="success" />,
        type: ViewerActionType.StatusMessage,
      });
      try {
        const store = (await ZipStorage.fromBlob(target.files[0]))
        if (store.initialized) {
          dispatch({
            text: null,
            icon: null,
            type: ViewerActionType.StatusMessage,
          });
          setDataFile(target.files[0]);
        } else {
          dispatch({
            text: "Failed to load from file",
            icon: <ErrorIcon color="error" />,
            type: ViewerActionType.StatusMessage,
          });
        }
      } catch(err) {
        dispatch({
          text: `Failed to load from file: ${err}`,
          icon: <ErrorIcon color="error" />,
          type: ViewerActionType.StatusMessage,
        });
      }
    } else {
      dispatch({
        text: null,
        icon: null,
        type: ViewerActionType.StatusMessage,
      });
      setDataFile(null);
    }
  };
  return (
    <>
      {setDataUrl && <RemoteUrlModal onLoad={setDataUrl} />}
      <Button
        component="label"
        variant="contained"
        tabIndex={-1}
        startIcon={<FileOpenIcon />}
      >
        {dataFile ? dataFile.name : "Choose File"}
        <VisuallyHiddenInput
          type="file"
          onChange={onChangeHandler}
          multiple
          accept=".mzpeak"
        />
      </Button>
    </>
  );
}
