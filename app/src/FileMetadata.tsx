import * as React from "react";
import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogTitle from "@mui/material/DialogTitle";
import { useSpectrumViewer } from "./util";
import * as mzpeakts from 'mzpeakts';
import {
  FileDescription,
  InstrumentConfiguration,
  Software,
  DataProcessingMethod,
} from "mzpeakts/src/metadata";


function NotOpenedBody() {
  return <>Open a file to view the file metadata</>;
}

function ParamValuePre(props: {value: any}) {
  return <pre style={{display: "inline-block", marginBlock: "0em 0em"}}>{props.value}</pre>
}

function paramListElement(p: mzpeakts.Param, keyPrefix: string|number|undefined=undefined) {
  const key = `${keyPrefix}-${p.name}-${p.accession}-${p.unit}`
  if (p.accession) {
    if (p.value) {
      return (
        <li key={key}>
          {p.name} ({p.accession}): <ParamValuePre value={p.value} />
        </li>
      );
    } else {
      return (
        <li key={key}>
          {p.name} ({p.accession})
        </li>
      );
    }
  } else {
    if (p.value) {
      return (
        <li key={key}>
          {p.name}: <ParamValuePre value={p.value} />
        </li>
      );
    } else {
      return <li key={key}>{p.name}</li>;
    }
  }
}

function FileDescriptionContent(props: {description: FileDescription}) {
  const sourceFiles = props.description.sourceFiles.map(sf => {
    return (
      <li key={`${sf.id}-${sf.location}-${sf.name}`}>
        <span>{sf.name}</span> ({sf.location}): {sf.id}
        <ul>{sf.params.map((p) => paramListElement(p, `fd-${sf.id}-`))}</ul>
      </li>
    );
  })
  return (
    <div>
      <h2>File Description</h2>
      <ul>
        {props.description.contents.map(p => paramListElement(p, "fd-"))}
      </ul>
      <h4>Source Files</h4>
      <ul>{sourceFiles}</ul>
    </div>
  );
}

function InstrumentConfigurationContent(props: {config: InstrumentConfiguration}) {
  const config = props.config
  console.log(config)
  return (
    <>
      <div className="metadata-section" key={`instrument-config-${config.id}`}>
        <h4>Instrument Configuration: {config.id}</h4>
        <div>Software Ref: {config.softwareReference}</div>
        <ul>{config.parameters.map(paramListElement)}</ul>
        <div>Components</div>
        <ul>
          {config.components.map(c => {
            return (
              <li key={`${c.componentType}-${c.order}`}>
                <div>{c.componentType} ({c.order})</div>
                <ul>{c.parameters.map(paramListElement)}</ul>
              </li>
            );
          })}
        </ul>
      </div>
    </>
  );
}

function SoftwareContent(props: {sw: Software}) {
  const sw = props.sw
   return (
     <>
       <div
         className="metadata-section"
         key={`software-${sw.id}`}
       >
         <h4>Software: {sw.id}</h4>
         <div>Version: {sw.version}</div>
         <ul>{sw.parameters.map(paramListElement)}</ul>
       </div>
     </>
   );
}

function SampleContent(props: {sample: mzpeakts.Sample}) {
  const sample = props.sample;
  return (
    <>
      <div className="metadata-section" key={`sample-${sample.id}`}>
        <h4>
          Sample ({sample.id}): {sample.name}
        </h4>
        <ul>
          {sample.parameters.map(p => paramListElement(p, "sample-"))}
        </ul>
      </div>
    </>
  );
}

function DataProcessingMethodContent(props: {dp: DataProcessingMethod}) {
  const dp = props.dp
  const methods = dp.methods.map(pm => {
    return <li key={`pm-${dp.id}-pm-${pm.order}`}>
      Order {pm.order}
      <ul>
        {pm.params.map(paramListElement)}
      </ul>
    </li>
  })
  return (<>
    <div className="metadata-section" key={`dataproc-${dp.id}`}>
      <h4>Data Processing: {dp.id}</h4>
      <ul>
        {methods}
      </ul>
    </div>
  </>)
}

function FileMetadataBody(props: {fileMetadata: mzpeakts.FileMetadata}) {
  const fileMetadata = props.fileMetadata;
  return (
    <>
      <div>
        <FileDescriptionContent
          description={fileMetadata.fileDescription}
        />
        <h2>Instrument Configurations</h2>
        {Array.from(fileMetadata?.instrumentConfigurations).sort((a, b) => a.id - b.id).map((ic) =>
          InstrumentConfigurationContent({ config: ic }),
        )}
        {fileMetadata?.software?.map((sw) =>
          SoftwareContent({ sw }),
        )}
        {fileMetadata?.dataProcessingMethods?.map(
          dp => DataProcessingMethodContent({dp}),
        )}
        {fileMetadata?.samples?.map(
          sample => SampleContent({sample})
        )}
      </div>
    </>
  );
}


export default function FileMetadataDialog() {
  const [open, setOpen] = React.useState(false);
  const state = useSpectrumViewer()

  const handleClickOpen = () => () => {
    setOpen(true);
  };

  const handleClose = () => {
    setOpen(false);
  };

  const descriptionElementRef = React.useRef<HTMLElement>(null);
  React.useEffect(() => {
    if (open) {
      const { current: descriptionElement } = descriptionElementRef;
      if (descriptionElement !== null) {
        descriptionElement.focus();
      }
    }
  }, [open]);

  let body = <NotOpenedBody key="file-metadata-1" />
  if (state.mzReader && state.mzReader?.fileMetadata) {
    body = <FileMetadataBody fileMetadata={state.mzReader.fileMetadata} key="file-metadata-1" />
  }

  return (
    <React.Fragment>
      <Button
        onClick={handleClickOpen()}
        component="label"
        variant="contained"
        tabIndex={-1}
        style={{ marginRight: "1em" }}
        disabled={state.mzReader ? false : true}
      >
        File Metadata
      </Button>
      <Dialog
        open={open}
        onClose={handleClose}
        scroll="paper"
        aria-labelledby="scroll-dialog-title"
        aria-describedby="scroll-dialog-description"
        fullWidth={true}
        maxWidth={"md"}
      >
        <DialogTitle id="scroll-dialog-title">File Metadata</DialogTitle>
        <DialogContent dividers={true}>

          <DialogContentText
            id="scroll-dialog-description"
            ref={descriptionElementRef}
            tabIndex={-1}
            component="section"
          >
            {body}
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleClose}>Dismiss</Button>
        </DialogActions>
      </Dialog>
    </React.Fragment>
  );
}
