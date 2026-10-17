import { renderToStaticMarkup } from "react-dom/server";
import { expect, it } from "vitest";
import { CollectionCount } from "./CollectionCount";

it("preserves YouTube's rounded source total and its provenance", () => {
  const container = document.createElement("div");
  container.innerHTML = renderToStaticMarkup(
    <CollectionCount
      count={144}
      total={26000}
      reportedText="26K videos"
      unit="videos"
      source="YouTube"
    />,
  );

  expect(container.firstElementChild?.getAttribute("title")).toBe(
    "144 videos collected / 26K videos reported by YouTube at last capture",
  );
  expect(container.querySelector(".collection-count-values")?.textContent).toBe(
    "144/≈26Kvideos",
  );
});

it("does not invent a source total for a channel without captured metadata", () => {
  const container = document.createElement("div");
  container.innerHTML = renderToStaticMarkup(
    <CollectionCount count={0} unit="videos" source="YouTube" />,
  );

  expect(container.firstElementChild?.getAttribute("title")).toBe(
    "0 videos collected",
  );
  expect(container.querySelector(".collection-count-values")?.textContent).toBe(
    "0collected",
  );
});
