import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'

export * from "./index.ts";

import * as mzpeakts from "mzpeakts";

(globalThis as any).mzpeakts = mzpeakts;

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
