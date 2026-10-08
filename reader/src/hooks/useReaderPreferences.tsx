import { createContext, type ReactNode, useContext, useState } from "react";
import {
  loadPreferences,
  type ReaderPreferences,
  savePreferences,
} from "../lib/preferences";

interface PreferencesContext {
  preferences: ReaderPreferences;
  updatePreferences: (next: ReaderPreferences) => void;
  preferencesError: boolean;
}

const Context = createContext<PreferencesContext | null>(null);

export function ReaderPreferencesProvider({
  children,
}: {
  children: ReactNode;
}) {
  const [preferences, setPreferences] = useState(loadPreferences);
  const [preferencesError, setPreferencesError] = useState(false);

  function updatePreferences(next: ReaderPreferences) {
    setPreferencesError(!savePreferences(next));
    setPreferences(next);
  }

  return (
    <Context.Provider
      value={{ preferences, updatePreferences, preferencesError }}
    >
      {children}
    </Context.Provider>
  );
}

export function useReaderPreferences() {
  const context = useContext(Context);
  if (!context) throw new Error("Reader preferences provider is missing.");
  return context;
}
