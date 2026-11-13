import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import { ReaderErrorBoundary } from "./components/ReaderErrorBoundary";
import { ReaderPreferencesProvider } from "./hooks/useReaderPreferences";
import "@fontsource-variable/dm-sans";
import "@fontsource-variable/dm-sans/wght-italic.css";
import "@fontsource-variable/newsreader/standard.css";
import "@fontsource-variable/newsreader/standard-italic.css";
import "./styles.css";
import "./youtube.css";
import "./hacker-news.css";

const client = new QueryClient({
  defaultOptions: {
    queries: { staleTime: 60_000, retry: 1, refetchOnWindowFocus: false },
  },
});
const root = document.getElementById("root");
if (root)
  createRoot(root).render(
    <StrictMode>
      <QueryClientProvider client={client}>
        <ReaderPreferencesProvider>
          <ReaderErrorBoundary>
            <App />
          </ReaderErrorBoundary>
        </ReaderPreferencesProvider>
      </QueryClientProvider>
    </StrictMode>,
  );
