import { useLayoutEffect, useState } from "react";
import { useReaderPreferences } from "../hooks/useReaderPreferences";
import type { ArticleImage } from "../lib/article";
import { analyzeImage } from "../lib/imageAnalysis";

export function ImageBackgroundToggle({ image, frame }: ArticleImage) {
  const { preferences } = useReaderPreferences();
  const automatic = preferences.autoImageBackground;
  const [analysis, setAnalysis] = useState({
    transparent: false,
    preferLight: false,
  });
  const [manual, setManual] = useState<boolean>();
  const light =
    analysis.transparent && (manual ?? (automatic && analysis.preferLight));

  useLayoutEffect(() => {
    const controller = new AbortController();
    const update = () => {
      const result = analyzeImage(image, automatic);
      setAnalysis(result);

      if (!result.transparent) setManual(undefined);
    };

    image.addEventListener("load", update, { signal: controller.signal });
    image.addEventListener("error", update, { signal: controller.signal });

    if (image.complete) update();

    return () => controller.abort();
  }, [image, automatic]);

  useLayoutEffect(() => {
    // The image is imported HTML; synchronize only its reader-owned presentation.
    frame.classList.toggle("article-image-frame", analysis.transparent);
    image.classList.toggle("article-image-light", light);

    return () => {
      frame.classList.remove("article-image-frame");
      image.classList.remove("article-image-light");
    };
  }, [frame, image, analysis.transparent, light]);

  if (!analysis.transparent) return null;

  return (
    <button
      type="button"
      className="image-background-toggle"
      aria-pressed={light}
      title="Toggle a light background behind this image"
      onClick={() => setManual(!light)}
    >
      Light background
    </button>
  );
}
