import { expect, it } from "vitest";
import { renderArticle } from "./article";
import {
  displayedRedditImages,
  enhanceRedditImageLinks,
  redditImageKey,
  withoutRedditImageUrls,
} from "./redditImages";

const original = "https://i.redd.it/first.jpg";
const second = "https://i.redd.it/second.jpg";
const gallery = "https://www.reddit.com/gallery/abc";
const images = [
  { url: "/assets/first.jpg", source_url: original },
  { url: "/assets/second.jpg", source_url: second },
];

it("matches original and rotating image previews without matching other hosts or galleries", () => {
  expect(
    redditImageKey("https://preview.redd.it/first.jpg?width=640&s=new"),
  ).toBe(redditImageKey(original));
  expect(redditImageKey("https://old.reddit.com/gallery/abc/?context=3")).toBe(
    redditImageKey(gallery),
  );
  expect(redditImageKey("https://i.redd.it.evil.test/first.jpg")).not.toBe(
    redditImageKey(original),
  );
  expect(redditImageKey("https://user@i.redd.it/first.jpg")).toBeUndefined();
  const displayed = displayedRedditImages(
    images,
    new Set([redditImageKey(images[0].url) ?? ""]),
    "t3_abc",
  );
  expect(withoutRedditImageUrls(`${original} ${gallery}`, displayed)).toBe("");
  expect(withoutRedditImageUrls(second, displayed)).toBe(second);
  expect(
    withoutRedditImageUrls("https://www.reddit.com/gallery/other", displayed),
  ).toContain("other");
  expect(withoutRedditImageUrls(`(${original}).`, displayed)).toBe("().");
});

it("restores failed image and gallery links while preserving image nodes, descriptive links and code", () => {
  const element = document.createElement("div");
  renderArticle(
    element,
    `<img src="/assets/first.jpg"><img src="/assets/second.jpg"><p><a href="${original}">${original}</a></p><p><a href="${second}">${second}</a></p><p><a href="${gallery}">${gallery}</a></p><p>Read <a href="${original}">the diagram</a>.</p><pre>${original}</pre><p>Plain ${original}.</p>`,
  );
  const nodes = [...element.querySelectorAll("img")];
  for (const image of nodes)
    Object.defineProperty(image, "complete", { value: false });
  const dispose = enhanceRedditImageLinks(element, images, "t3_abc");
  const raw = [...element.querySelectorAll<HTMLAnchorElement>("a")].find(
    (a) => a.textContent === original,
  );
  const rawGallery = [...element.querySelectorAll<HTMLAnchorElement>("a")].find(
    (a) => a.textContent === gallery,
  );
  expect(raw?.hidden).toBe(true);
  expect(rawGallery?.hidden).toBe(true);
  expect(element.querySelector("pre")?.textContent).toBe(original);
  expect(element.querySelector(`a[href="${original}"]`)?.textContent).toBe(
    original,
  );
  expect(
    element.querySelector('a:not([hidden])[href="https://i.redd.it/first.jpg"]')
      ?.textContent,
  ).toBe("the diagram");
  nodes[1].dispatchEvent(new Event("error"));
  expect(raw?.hidden).toBe(true);
  expect(rawGallery?.hidden).toBe(false);
  expect(element.querySelectorAll("img")[0]).toBe(nodes[0]);
  nodes[0].dispatchEvent(new Event("error"));
  expect(raw?.hidden).toBe(false);
  dispose();
  nodes[0].dispatchEvent(new Event("load"));
  expect(raw?.hidden).toBe(false);
});

it("keeps references without a matching clickable image, including partial galleries", () => {
  const element = document.createElement("div");
  renderArticle(
    element,
    `<a href="https://example.com/unrelated"><img src="/assets/first.jpg"></a><p><a href="${original}">${original}</a></p><p><a href="${gallery}">${gallery}</a></p>`,
  );
  const dispose = enhanceRedditImageLinks(element, images, "t3_abc");
  expect(element.querySelectorAll("[hidden]")).toHaveLength(0);
  dispose();
});
