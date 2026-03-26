import { useState, useEffect, Fragment } from 'react'
import './App.css'

import { DataFileChooser } from "./DataFileChooser";
import { MZPeakReader } from "mzpeakts";
import { SpectrumList } from './SpectrumList';
import { SpectrumCanvasComponent2 } from "./canvas/component"
import {
    SpectrumViewerProvider,
    useSpectrumViewer,
    useSpectrumViewerDispatch,
    ViewerActionType,
} from "./util";

import useMediaQuery from "@mui/material/useMediaQuery";
import AppBar from "@mui/material/AppBar";
import Toolbar from "@mui/material/Toolbar";
import Typography from "@mui/material/Typography";
import { createTheme, styled } from '@mui/material/styles';
import Drawer from "@mui/material/Drawer";
import Divider from "@mui/material/Divider";
import InstructionsDialog from "./Instructions"
import { ThemeProvider } from '@emotion/react';

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

  return (
    <>
      <Header>
        <InstructionsDialog />
        <DataFileChooser dataFile={dataFile} setDataFile={setDataFile} />
      </Header>

      <div>
        <SpectrumCanvasComponent2 />
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
