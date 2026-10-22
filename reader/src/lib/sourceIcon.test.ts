import { expect, it } from "vitest";
import { sourceIconFit } from "./sourceIcon";

function artwork(
  size: number,
  left: number,
  top: number,
  width: number,
  height: number,
) {
  const pixels = new Uint8ClampedArray(size * size * 4);

  for (let y = top; y < top + height; y += 1) {
    for (let x = left; x < left + width; x += 1) {
      pixels[(y * size + x) * 4 + 3] = 255;
    }
  }

  return pixels;
}

it("fits padded artwork to the same visible size without changing the pixels", () => {
  const pixels = artwork(8, 2, 2, 4, 4);
  const original = pixels.slice();
  const fit = sourceIconFit(pixels, 8);
  expect(fit).toEqual({ scale: 2, x: 0, y: 0 });
  expect(pixels).toEqual(original);
});

it("centres asymmetric padding while preserving a rectangular logo's proportions", () => {
  const fit = sourceIconFit(artwork(8, 1, 0, 4, 2), 8);
  expect(fit).toEqual({ scale: 2, x: 12.5, y: 37.5 });
  expect(4 * (fit?.scale ?? 0)).toBe(8);
  expect(2 * (fit?.scale ?? 0)).toBe(4);
});

it("keeps full-size artwork unchanged, including its antialiased edge", () => {
  const pixels = artwork(8, 1, 1, 6, 6);
  pixels[3] = 1;
  pixels[pixels.length - 1] = 1;
  expect(sourceIconFit(pixels, 8)).toEqual({ scale: 1, x: 0, y: 0 });
});

it("leaves wholly transparent icons alone", () => {
  expect(sourceIconFit(new Uint8ClampedArray(8 * 8 * 4), 8)).toBeUndefined();
});
