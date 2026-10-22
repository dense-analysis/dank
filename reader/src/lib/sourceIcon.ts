const SAMPLE_SIZE = 64;

export function sourceIconFit(pixels: Uint8ClampedArray, size: number) {
  let left = size;
  let top = size;
  let right = -1;
  let bottom = -1;

  for (let y = 0; y < size; y += 1) {
    for (let x = 0; x < size; x += 1) {
      if (!pixels[(y * size + x) * 4 + 3]) continue;

      left = Math.min(left, x);
      top = Math.min(top, y);
      right = Math.max(right, x);
      bottom = Math.max(bottom, y);
    }
  }

  if (right < left) return undefined;

  return {
    scale: size / Math.max(right - left + 1, bottom - top + 1),
    x: ((size - left - right - 1) / (2 * size)) * 100,
    y: ((size - top - bottom - 1) / (2 * size)) * 100,
  };
}

export function sourceIconTransform(image: HTMLImageElement) {
  if (!image.naturalWidth || !image.naturalHeight) return undefined;

  try {
    const canvas = document.createElement("canvas");
    canvas.width = canvas.height = SAMPLE_SIZE;
    const context = canvas.getContext("2d");
    if (!context) return undefined;

    // Match object-fit: contain before measuring transparent margins.
    const ratio =
      SAMPLE_SIZE / Math.max(image.naturalWidth, image.naturalHeight);
    const width = image.naturalWidth * ratio;
    const height = image.naturalHeight * ratio;
    context.drawImage(
      image,
      (SAMPLE_SIZE - width) / 2,
      (SAMPLE_SIZE - height) / 2,
      width,
      height,
    );
    const fit = sourceIconFit(
      context.getImageData(0, 0, SAMPLE_SIZE, SAMPLE_SIZE).data,
      SAMPLE_SIZE,
    );
    if (!fit) return undefined;

    return `scale(${fit.scale}) translate(${fit.x}%, ${fit.y}%)`;
  } catch {
    // Uninspectable images retain their ordinary contained size.
    return undefined;
  }
}
