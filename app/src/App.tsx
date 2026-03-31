import { useState, useEffect, Fragment } from 'react'
import './App.css'

import { DataFileChooser } from "./DataFileChooser";
import { MZPeakReader } from "mzpeakts";
import { SpectrumList } from './SpectrumList';
import { SpectrumCanvasComponent } from "./canvas/component"
import {
    SpectrumViewerProvider,
    useSpectrumViewer,
    useSpectrumViewerDispatch,
    ViewerActionType,
} from "./util";

import AppBar from "@mui/material/AppBar";
import Box from "@mui/material/Box";
import Toolbar from "@mui/material/Toolbar";
import Typography from "@mui/material/Typography";
import { createTheme, styled } from '@mui/material/styles';
import InstructionsDialog from "./Instructions"
import { ThemeProvider } from '@emotion/react';
import ErrorIcon from '@mui/icons-material/Error';

const theme = createTheme({
  colorSchemes: {
    light: {
      palette: {
        primary: {
          main: "#2da1be",
          light: "#7fd3e7",
          dark: "#5b51e6",
        },
        secondary: {
          main: "#448ec0",
          light: "#4dd8c1",
          dark: "#0c9780",
        },
      },
    },
  },
  components: {
    // Name of the component
    MuiButtonBase: {
      defaultProps: {
        // The props to change the default for.
        color: "#4a8f2e",
      },
    },
  },
});

export const Offset = styled("div")(({ theme }) => theme.mixins.toolbar);

interface HeaderProps {
    children: string | JSX.Element | JSX.Element[]
}

function StatusDisplay() {
  const { statusMessage } = useSpectrumViewer();
  if (!statusMessage.text && !statusMessage.icon) return null;
  return (
    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mr: 2 }}>
      {statusMessage.icon}
      {statusMessage.text && (
        <Typography variant="body2">{statusMessage.text}</Typography>
      )}
    </Box>
  );
}

export function Header({ children }: HeaderProps) {
  return (
    <Fragment>
      <AppBar position="fixed" id="application-header" style={{ zIndex: 999 }} color='secondary'>
        <Toolbar>
          <Typography variant="h6" component="div" sx={{ flexGrow: 1 } }>
            <code>mzPeaks</code> Demo
          </Typography>
          {children}
        </Toolbar>
      </AppBar>
      <Offset />
    </Fragment>
  );
}


export function Frame() {
  const [dataFile, setDataFile] = useState<File | null>(null);
  const [dataUrl, setDataUrl] = useState<string | null>(null);
  const viewStateDispatch = useSpectrumViewerDispatch();
  const viewState = useSpectrumViewer();

  console.log("Base State", viewState);

  useEffect(() => {
    if (dataFile) {
      console.log("Opening", dataFile);
      MZPeakReader.fromBlob(dataFile).then((value) => {
        viewStateDispatch({
          type: ViewerActionType.MZReader,
          value,
        });
      });
    } else {
      viewStateDispatch({
        type: ViewerActionType.MZReader,
        value: null,
      });
    }
  }, [dataFile]);

  useEffect(() => {
    if (dataUrl) {
      viewStateDispatch({type: ViewerActionType.StatusMessage, text: `Loading from URL: ${dataUrl}`})
      MZPeakReader.fromUrl(dataUrl).then((value) => {
        viewStateDispatch({
          type: ViewerActionType.StatusMessage,
          text: null,
          icon: null,
        });
        viewStateDispatch({ type: ViewerActionType.MZReader, value });
      }).catch((err) => {
        viewStateDispatch({
          type: ViewerActionType.StatusMessage,
          text: `Failed to load file from URL: ${err}`,
          icon: <ErrorIcon color="error" />,
        });
      });
    }
  }, [dataUrl]);

  return (
    <>
      <Header>
        <StatusDisplay />
        <InstructionsDialog />
        <DataFileChooser dataFile={dataFile} setDataFile={setDataFile} setDataUrl={setDataUrl} />
      </Header>

      <div>
        <SpectrumCanvasComponent />
      </div>

      <div>{viewState.mzReader ? <SpectrumList /> : <div></div>}</div>
    </>
  );
}


function App() {
    return (
      <ThemeProvider theme={theme}>
      <SpectrumViewerProvider>
        <Frame />
      </SpectrumViewerProvider>
      </ThemeProvider>
    );
}

export default App
