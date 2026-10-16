import { Check, CircleAlert, RotateCcw } from "lucide-react";
import { useReaderPreferences } from "../hooks/useReaderPreferences";
import {
  defaultPreferences,
  type ReadingFont,
  readingFonts,
} from "../lib/preferences";

export function ReaderSettings() {
  const { preferences, updatePreferences, preferencesError } =
    useReaderPreferences();

  return (
    <section className="reader-settings" aria-label="Reading preferences">
      <fieldset className="font-options">
        <legend>Reading font</legend>
        <p className="settings-help">
          For story titles, previews and articles. Navigation stays familiar.
        </p>
        <div className="font-choices">
          {(Object.keys(readingFonts) as ReadingFont[]).map((font) => (
            <label
              className={`font-choice ${preferences.font === font ? "selected" : ""}`}
              key={font}
            >
              <input
                type="radio"
                name="reading-font"
                value={font}
                checked={preferences.font === font}
                onChange={() => updatePreferences({ ...preferences, font })}
              />
              <span
                className="font-sample"
                style={{ fontFamily: readingFonts[font].family }}
                aria-hidden="true"
              >
                Ag
              </span>
              <strong>{readingFonts[font].name}</strong>
              <small>{readingFonts[font].description}</small>
            </label>
          ))}
        </div>
      </fieldset>
      <div className="size-setting">
        <div className="setting-label">
          <label htmlFor="article-size">Article text size</label>
          <output htmlFor="article-size">{preferences.fontSize} px</output>
        </div>
        <input
          id="article-size"
          type="range"
          min="16"
          max="26"
          step="1"
          value={preferences.fontSize}
          onChange={(event) =>
            updatePreferences({
              ...preferences,
              fontSize: Number(event.target.value),
            })
          }
        />
        <div className="range-labels" aria-hidden="true">
          <span>Smaller</span>
          <span>Larger</span>
        </div>
      </div>
      <div className="image-background-setting">
        <label htmlFor="auto-image-background">
          <input
            id="auto-image-background"
            type="checkbox"
            checked={preferences.autoImageBackground}
            aria-describedby="auto-image-background-help"
            onChange={(event) =>
              updatePreferences({
                ...preferences,
                autoImageBackground: event.target.checked,
              })
            }
          />
          Automatically improve image backgrounds
        </label>
        <p className="settings-help" id="auto-image-background-help">
          Use a light background when transparent images would otherwise be
          difficult to read. You can still change each image’s background.
        </p>
      </div>
      <fieldset className="reddit-reading-settings">
        <legend>Reddit reading</legend>
        <div className="image-background-setting">
          <label htmlFor="show-reddit-comments">
            <input
              id="show-reddit-comments"
              type="checkbox"
              checked={preferences.showRedditComments}
              aria-describedby="show-reddit-comments-help"
              onChange={(event) =>
                updatePreferences({
                  ...preferences,
                  showRedditComments: event.target.checked,
                })
              }
            />
            Show captured comments
          </label>
          <p className="settings-help" id="show-reddit-comments-help">
            Read saved conversations beneath Reddit posts. Hiding them keeps the
            archived comments intact.
          </p>
        </div>
        <div className="image-background-setting">
          <label htmlFor="show-remote-content-labels">
            <input
              id="show-remote-content-labels"
              type="checkbox"
              checked={preferences.showRemoteContentLabels}
              aria-describedby="show-remote-content-labels-help"
              onChange={(event) =>
                updatePreferences({
                  ...preferences,
                  showRemoteContentLabels: event.target.checked,
                })
              }
            />
            Show remote content labels
          </label>
          <p className="settings-help" id="show-remote-content-labels-help">
            Mark videos that stream from Reddit and haven’t been saved for
            offline viewing.
          </p>
        </div>
        <p className="settings-help">
          These are reading preferences. Video downloads, comment collection,
          comment limits and sorting are controlled by your collection
          configuration.
        </p>
      </fieldset>
      <section className="reading-preview" aria-label="Reading preview">
        <p className="eyebrow">A LITTLE PREVIEW</p>
        <h2>A quieter corner of the internet.</h2>
        <p className="preview-body">
          Good stories deserve a little room to breathe. Bring your favourite
          voices together, follow your curiosity, and settle into something
          worth your time.
        </p>
        <p className="preview-caption">Your reading, at your own pace.</p>
      </section>
      <div className="settings-footer">
        <p role="status">
          <Check size={15} /> Changes apply immediately.
        </p>
        <button
          type="button"
          className="text-button"
          onClick={() => updatePreferences({ ...defaultPreferences })}
        >
          <RotateCcw size={14} /> Reset to defaults
        </button>
      </div>
      <p className="settings-help">Your preferences stay in this browser.</p>
      {preferencesError && (
        <div className="error-banner" role="alert">
          <CircleAlert size={16} /> Browser storage is unavailable or full.
          These preferences will last until this page closes.
        </div>
      )}
    </section>
  );
}
