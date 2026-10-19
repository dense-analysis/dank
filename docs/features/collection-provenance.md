# Article collection and discovery history

DANK keeps a publisher article separate from the posts that link to it. The
reader can reuse its saved excerpt across feeds while preserving each post's
own text, discussion and source identity. Collection mechanisms can differ;
equivalent saved content should have consistent presentation.

## Collect publisher pages from Reddit

Enable the following in private `config.toml`:

```toml
[reddit]
collect_linked_articles = true
```

The default is `false`. When enabled, Reddit collection saves its posts first,
then collects their linked publisher pages. Readable originals already saved by
RSS, Hacker News or another Reddit community are reused. Missing or empty
originals use the existing article and media pipeline. Social platforms, video
platforms and Reddit self-posts are not treated as publisher pages.

To backfill the latest saved link posts from configured communities without
recollecting Reddit listings:

```sh
make scrape ARGS='--reddit-linked-articles'
make process ARGS='--age 24h --domains "^reddit\.com$"'
```

Add `MODE=native` for native execution. Backfill respects the setting and the
configured maximum posts per community. Turning collection off stops future
publisher requests; saved articles, excerpts and history remain available.
Blocked pages and unsupported documents retain explicit unavailable captures.

## What the collection indicator means

The eye beside **Open original** opens **Collection info**. It is highlighted
when DANK had already observed the article before observing this particular
post's link to it. The tooltip names the earliest observed source and time.
The same disclosure is available in the opened post.

The panel distinguishes:

- **First observed:** the earliest saved post link or direct article capture
  known to DANK, including the referring community when recorded.
- **This post first collected:** the first raw capture of this post identity.
- **This link first observed:** the first saved observation of this post linking
  to this article. This can be later if the post was edited.
- **Publisher page collected:** the first successful saved page capture that
  has been processed for display.
- **Collected via:** the actual collection route recorded when the page was
  fetched. New Reddit and HN fetches retain the referring post, community and
  observation time. Older linked captures without attribution remain unknown.

These are collection timestamps, not publication timestamps or a claim about
who reported a story first. An old article collected today does not become an
early discovery merely because its publication date is old. Tied first
observations are retained as ties. Earliest source labels are bounded to five;
missing historical captures can leave timing unknown.

Exact URLs and recorded redirects establish article identity. Similar titles
alone do not merge articles. A previously saved article can be reused without
claiming that the new referring source collected it. A new raw capture waiting
for processing does not upgrade the status of an older displayed capture.

## Connections follow the viewing context

From Reddit, Connections includes Reddit references. A community view narrows
those references to that community, including when you explore its saved
publisher article. X, Hacker News and YouTube contexts similarly keep their
own source scope. All and Blogs allow broader cross-source exploration.
Search, date and flair filters narrow the feed, not its historical evidence;
older references remain available in Connections. A selected publisher-domain
filter does not further narrow provenance references.

A saved publisher article appears before the contextual source/community and
current post in the graph. Other saved posts linking to that article can appear
within the same context. Expansion remains bounded and one hop deep; unrelated
sources do not flood a community's graph. Empty publisher captures remain
unavailable rather than appearing as a successfully collected article.

Graph layout expresses the article's relationship to its sources, not a
timeline. Arrows retain the direction of observed links. Collection info supplies
actual first-observed timing; a link does not imply copying, influence or
causation. Counts reflect the selected scope and disclose bounded results.

## Future analysis: source novelty and a repost index

A future analysis view could compare how often communities or feeds introduce
articles that DANK has not seen, and how quickly they reference already known
articles. Useful candidates include:

- The share of eligible article links already observed elsewhere before this
  source's first observation of the same article: a possible **repost index**.
- Median observed delay from the first known source to this source, with ties
  reported separately.
- New-to-DANK article share, repeated links within a community, and coverage by
  publisher or topic.

This is a future feature consideration, not an implemented score. Define the
unit explicitly before calculating it: unique article/source observations over
a stated window, separate from repeated scraping, comments and within-source
duplicates. Show the number of eligible observations and unknown or unmatched
links beside any ratio.

Polling frequency, ranking filters, selected communities, outages, access
failures, archive imports and when a source was first enabled all bias observed
order. Comparisons need collection coverage and comparable time windows;
publication dates and post dates should be separate dimensions. A late
collection is not evidence that a community copied a story or lacks original
reporting. Preserve the underlying evidence and uncertainty before considering
ranking feeds as leading or repost-heavy.
