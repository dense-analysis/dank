import { Plus, X } from "lucide-react";
import { useId, useState } from "react";
import type { SourceTarget } from "../lib/sourceSettings";

/** Splits pasted text into entries: one per line, comma or space. */
function entries(text: string): string[] {
  return text
    .split(/[\s,]+/)
    .map((value) => value.trim())
    .filter(Boolean);
}

/**
 * One list of a source's accounts, feeds or communities. Each entry can be
 * switched off without losing it; the server normalizes what is added.
 */
export function SourceTargets({
  label,
  one,
  placeholder,
  help,
  targets,
  change,
  display,
  message,
  entryMessage,
}: {
  label: string;
  one: string;
  placeholder: string;
  help: string;
  targets: readonly SourceTarget[];
  change: (targets: SourceTarget[]) => void;
  display: (value: string) => string;
  /** A problem with the list as a whole. */
  message?: string;
  entryMessage: (index: number) => string | undefined;
}) {
  const id = useId();
  const [text, setText] = useState("");

  function add() {
    const known = new Set(targets.map((target) => target.value));
    const fresh = [...new Set(entries(text))].filter(
      (value) => !known.has(value),
    );

    if (fresh.length) {
      change([...targets, ...fresh.map((value) => ({ value, enabled: true }))]);
    }

    setText("");
  }

  return (
    <fieldset className="source-targets">
      <legend>{label}</legend>
      <p className="settings-help" id={`${id}-help`}>
        {help}
      </p>
      {message && (
        <p className="field-error" role="alert">
          {message}
        </p>
      )}
      {targets.length > 0 && (
        <ul>
          {targets.map((target, index) => {
            const problem = entryMessage(index);

            return (
              // Entries have no IDs, and a list never holds a value twice.
              <li key={target.value} className={problem ? "invalid" : ""}>
                <label title={target.value}>
                  <input
                    type="checkbox"
                    checked={target.enabled}
                    aria-describedby={problem ? `${id}-${index}` : undefined}
                    aria-invalid={problem ? true : undefined}
                    onChange={(event) =>
                      change(
                        targets.map((item, at) =>
                          at === index
                            ? { ...item, enabled: event.target.checked }
                            : item,
                        ),
                      )
                    }
                  />
                  <span>{display(target.value)}</span>
                </label>
                <button
                  type="button"
                  className="icon-button"
                  aria-label={`Remove ${display(target.value)}`}
                  onClick={() =>
                    change(targets.filter((_, at) => at !== index))
                  }
                >
                  <X size={15} aria-hidden="true" />
                </button>
                {problem && (
                  <p className="field-error" id={`${id}-${index}`}>
                    {problem}
                  </p>
                )}
              </li>
            );
          })}
        </ul>
      )}
      <div className="source-add">
        <input
          type="text"
          value={text}
          placeholder={placeholder}
          aria-label={`Add ${/^[aeiou]/.test(one) ? "an" : "a"} ${one}`}
          aria-describedby={`${id}-help`}
          autoCapitalize="none"
          autoCorrect="off"
          spellCheck={false}
          onChange={(event) => setText(event.target.value)}
          onKeyDown={(event) => {
            if (event.key !== "Enter") return;

            // Enter adds the entry rather than saving the source.
            event.preventDefault();
            add();
          }}
          onBlur={add}
        />
        <button
          type="button"
          className="secondary-button"
          disabled={!entries(text).length}
          onClick={add}
        >
          <Plus size={15} aria-hidden="true" /> Add
        </button>
      </div>
    </fieldset>
  );
}
