type RGB = readonly [number, number, number];

// DANK's conservative policy choices, exercised by tests/image-backgrounds.spec.ts.
// These cutoffs are heuristics, not WCAG requirements or a guarantee of readability.
const thresholds = {
  opaqueAlpha: 250,
  clearAlpha: 16,
  minClearFraction: 0.65,
  minForegroundAlpha: 0.5,
  minForegroundPixels: 16,
  minPoorReaderFraction: 0.7,
  maxPoorWhiteFraction: 0.15,
  poorContrastRatio: 3,
} as const;

// WCAG's sRGB linearization and relative-luminance weights:
// https://www.w3.org/TR/WCAG22/#dfn-relative-luminance
function luminance(rgb: RGB): number {
  const linear = rgb.map((value) => {
    const channel = value / 255;

    return channel <= 0.04045
      ? channel / 12.92
      : ((channel + 0.055) / 1.055) ** 2.4;
  });

  return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2];
}

// WCAG contrast ratio, including the 0.05 offset:
// https://www.w3.org/TR/WCAG22/#dfn-contrast-ratio
function contrast(a: number, b: number): number {
  return (Math.max(a, b) + 0.05) / (Math.min(a, b) + 0.05);
}

function analyzeImagePixels(
  pixels: Uint8ClampedArray,
  background: RGB,
  automatic: boolean,
) {
  let transparent = false;
  let clearPixels = 0;

  for (let index = 3; index < pixels.length; index += 4) {
    if (pixels[index] < thresholds.opaqueAlpha) transparent = true;
    if (pixels[index] < thresholds.clearAlpha) clearPixels += 1;
  }

  // Only sparse transparent figures need the more expensive contrast check.
  if (
    !automatic ||
    clearPixels < (pixels.length / 4) * thresholds.minClearFraction
  ) {
    return { transparent, preferLight: false };
  }

  let foregroundPixels = 0;
  let poorOnReader = 0;
  let poorOnWhite = 0;
  const readerLuminance = luminance(background);

  for (let index = 0; index < pixels.length; index += 4) {
    const alpha = pixels[index + 3] / 255;

    // Very faint pixels and antialiasing should not dominate the contrast vote.
    if (alpha < thresholds.minForegroundAlpha) continue;

    foregroundPixels += 1;

    // Include transparency so faint strokes are measured as they would be displayed.
    const r = pixels[index] * alpha;
    const g = pixels[index + 1] * alpha;
    const b = pixels[index + 2] * alpha;
    const remaining = 1 - alpha;
    const onReader = luminance([
      r + background[0] * remaining,
      g + background[1] * remaining,
      b + background[2] * remaining,
    ]);
    const onWhite = luminance([
      r + 255 * remaining,
      g + 255 * remaining,
      b + 255 * remaining,
    ]);

    if (contrast(onReader, readerLuminance) < thresholds.poorContrastRatio) {
      poorOnReader += 1;
    }

    if (contrast(onWhite, 1) < thresholds.poorContrastRatio) {
      poorOnWhite += 1;
    }
  }

  return {
    transparent,
    // Require enough visible ink and a clear improvement on white.
    preferLight:
      foregroundPixels >= thresholds.minForegroundPixels &&
      poorOnReader / foregroundPixels >= thresholds.minPoorReaderFraction &&
      poorOnWhite / foregroundPixels <= thresholds.maxPoorWhiteFraction,
  };
}

export function analyzeImage(image: HTMLImageElement, automatic: boolean) {
  const unchanged = { transparent: false, preferLight: false };

  if (!image.naturalWidth || !image.naturalHeight) return unchanged;

  // Sample the decoded image, so SVG backgrounds and raster alpha work alike.
  const canvas = document.createElement("canvas");
  canvas.width = automatic ? 128 : 32;
  canvas.height = canvas.width;
  const context = canvas.getContext("2d", { willReadFrequently: true });

  if (!context) return unchanged;

  try {
    context.fillStyle = getComputedStyle(
      document.documentElement,
    ).backgroundColor;
    context.fillRect(0, 0, 1, 1);
    const background = context.getImageData(0, 0, 1, 1).data;

    context.clearRect(0, 0, canvas.width, canvas.height);
    context.drawImage(image, 0, 0, canvas.width, canvas.height);
    const { data } = context.getImageData(0, 0, canvas.width, canvas.height);

    return analyzeImagePixels(
      data,
      [background[0], background[1], background[2]],
      automatic,
    );
  } catch {
    // Cross-origin images without CORS remain readable, without extra requests.
  }

  return unchanged;
}
