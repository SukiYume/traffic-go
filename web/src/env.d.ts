/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_TRAFFICGO_USE_MOCK?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
