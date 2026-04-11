import { useState, useEffect, Fragment } from 'react'
import './App.css'

import { DataFileChooser } from "./DataFileChooser";
import { MZPeakReader } from "mzpeakts";
import { SpectrumList } from './SpectrumList';
import { ChromatogramCanvasComponent, SpectrumCanvasComponent } from "./canvas/component"
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
import FileMetadataDialog from "./FileMetadata"
import { ThemeProvider } from '@emotion/react';
import ErrorIcon from '@mui/icons-material/Error';
import { XICExtractDialog, XICTarget } from './XICExtract';
import Tabs from '@mui/material/Tabs';
import Tab from "@mui/material/Tab";
import { ChromatogramList } from './ChromatogramList';

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
      <AppBar
        position="fixed"
        id="application-header"
        style={{ zIndex: 999 }}
        color="secondary"
      >
        <Toolbar>
          <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
            <code>mzPeaks</code> Demo
          </Typography>
          {children}
        </Toolbar>
      </AppBar>
      <Offset id="offset-pad" />
    </Fragment>
  );
}

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`vertical-tabpanel-${index}`}
      aria-labelledby={`vertical-tab-${index}`}
      {...other}
    >
      {value === index && (
        <Box sx={{ p: 0 }}>
          {children}
        </Box>
      )}
    </div>
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

  const [xicSpec, setXICSpec] = useState<XICTarget | null>(null)

  useEffect(() => {
    if (xicSpec) {
      viewStateDispatch({
        type: ViewerActionType.StatusMessage,
        text: `Extracting XIC...`,
      });
      viewState.extractXIC(
        xicSpec.startTime,
        xicSpec.endTime,
        xicSpec.startMz,
        xicSpec.endMz).then((value) => {
          if (value == null) {
            viewStateDispatch({
              type: ViewerActionType.StatusMessage,
              text: `No spectrum data available, cannot extract XIC`,
            });
          } else {
            viewStateDispatch({
              type: ViewerActionType.XICExtract,
              target: value
            })
            viewStateDispatch({
              type: ViewerActionType.StatusMessage,
              text: null
            });
          }
        })
        setXICSpec(null)
    }
  })

  const [tabIndex, setTabIndex] = useState(0)

  return (
    <>
      <Header>
        <Tabs
          id="main-tabs"
          value={tabIndex}
          onChange={(_event, index) => setTabIndex(index)}
          indicatorColor="primary"
        >
          <Tab label="Mass Spectra" />
          <Tab label="Chromatograms" />
        </Tabs>
        <StatusDisplay />
        <XICExtractDialog
          onLoad={(e) => {
            console.log(e);
            setXICSpec(e);
          }}
        />
        <FileMetadataDialog />
        <DataFileChooser
          dataFile={dataFile}
          setDataFile={setDataFile}
          setDataUrl={setDataUrl}
        />
      </Header>

      <TabPanel value={tabIndex} index={0}>
        <div>
          <SpectrumCanvasComponent />
        </div>
        <div>{viewState.mzReader ? <SpectrumList /> : <div></div>}</div>
      </TabPanel>

      <TabPanel value={tabIndex} index={1}>
        <div>
          <ChromatogramCanvasComponent />
        </div>
        <div>{viewState.mzReader ? <ChromatogramList /> : <div></div>}</div>
      </TabPanel>
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
