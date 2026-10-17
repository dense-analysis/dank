import { useState } from "react";
import { createRoot } from "react-dom/client";
import YouTubeReferenceGraph from "../../src/components/YouTubeReferenceGraph";
import type {
  YouTubeItem,
  YouTubeReference,
} from "../../src/lib/youtubeCollection";
import "../../src/styles.css";

const reference: YouTubeReference = {
  domain: "reddit.com",
  source: "reddit",
  post_id: "reddit-discussion",
  title:
    "Drunken Godess Reflux Cancels Demo In Fear Of People Using AI To Copy The Game And Release It Before They Can",
  url: "https://www.reddit.com/r/gamedev/comments/reddit-discussion/",
  reader_url: "/reader/?article=reddit-discussion&article_source=reddit.com",
  kind: "comment",
  reference_id: "ph4vac2",
  reference_url:
    "https://www.reddit.com/r/gamedev/comments/reddit-discussion/comment/ph4vac2/",
  matched_url: "https://youtu.be/NnV_cWeoo5Q?t=1468",
  author: "James20k",
  excerpt:
    "...and they've seen it happen in real time. What I think we're really seeing is that these AI models have always been fully automated plagiarism.",
  observed_at: "2026-10-17T12:00:00Z",
};
const item: YouTubeItem = {
  id: "video:NnV_cWeoo5Q",
  video_id: "NnV_cWeoo5Q",
  kind: "video",
  title: "Kernel Recipes 2026 - Security in the LLM age",
  url: "https://www.youtube.com/watch?v=NnV_cWeoo5Q",
  text: "",
  channel: { id: "", name: "", url: "" },
  thumbnail_url: null,
  duration_text: "",
  views_text: "",
  published_text: "",
  observed_at: reference.observed_at,
  detail_capture: { status: "disabled" },
  discoveries: [],
  references: [
    {
      ...reference,
      domain: "news.ycombinator.com",
      source: "hacker_news",
      post_id: "50132950",
      reference_id: "50133642",
      author: "zdw",
      title: "Development statistics for the 7.2 kernel",
      excerpt:
        "Greg KH did a video on the flood of LLM created stuff: https://www.youtube.com/watch?v=NnV_cWeoo5Q",
      reference_url: "https://news.ycombinator.com/item?id=50133642",
    },
    reference,
  ],
  reference_count: 2,
};

function Fixture() {
  const [video, setVideo] = useState(item);
  const [opened, setOpened] = useState("");
  const [shown, setShown] = useState(true);

  return (
    <div style={{ maxWidth: 1040, margin: "32px auto", padding: "0 16px" }}>
      <h1>Links and mentions in your feeds</h1>
      <button
        type="button"
        onClick={() => setVideo({ ...video, title: "Updated YouTube title" })}
      >
        Update metadata
      </button>
      <button type="button" onClick={() => setShown(!shown)}>
        Toggle graph
      </button>
      <output aria-label="Opened reference">{opened}</output>
      {shown && (
        <YouTubeReferenceGraph
          item={video}
          onOpenReference={(selected) =>
            setOpened(`${selected.domain}:${selected.reference_id}`)
          }
        />
      )}
    </div>
  );
}
const container = document.getElementById("root");

if (container) createRoot(container).render(<Fixture />);
