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

The **Info** icon beside **Open original** opens **Collection info**. It is
available whenever saved collection metadata exists, even if this post is the
first known discovery. The same disclosure is available in the opened post.

A separate **eye** appears only when a different recorded source/post discovered
the exact destination before the linking post's known publication time. Its
tooltip says **Discovered at <source> on <time>**. The current post and an article
capture attributed to it do not count as independent earlier discoveries.
Unknown publication dates or unidentified legacy collectors do not qualify.

This is stricter than collection order: another source collected after the
Reddit post was published, but before DANK collected that Reddit post, does not
produce an eye. That collection-order difference remains in the information
panel. Native Reddit videos likewise need actual earlier evidence for the same
video URL; being a saved or playable video alone does not produce an eye.

The panel distinguishes:

- **First observed:** the earliest saved post link or direct article capture
  known to DANK, including the referring community when recorded.
- **This post published:** the source's known publication/submission timestamp,
  kept separate from DANK's collection time. Missing dates stay unknown.
- **This post first collected:** the first raw capture of this post identity.
- **This link first observed:** the first saved observation of this post linking
  to this article. This can be later if the post was edited.
- **Publisher page collected:** the first successful saved page capture that
  has been processed for display.
- **Collected via:** the actual collection route recorded when the page was
  fetched. New Reddit and HN fetches retain the referring post, community and
  observation time. Older linked captures without attribution remain unknown.

The panel distinguishes recorded collection timestamps from the post's
publication timestamp. The eye compares an independent discovery timestamp to
that known publication timestamp; the collection-order note compares two
collection timestamps. Neither comparison proves copying or who originally
reported a story. An old article collected today does not become an
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
timeline. Arrows retain the direction of observed links. A **First discovered**
node attached by a dashed line shows the earliest recorded source and local
date/time for the article. It can name a source outside the current community
without expanding that source's other posts or links. Selecting it shows the
exact timestamp and keeps the linking post's publication time separate. Tied
first sources remain ties; unknown sources or times remain explicit.

This history annotation uses the same saved provenance as Collection info. It
appears only while its matching destination is visible in the selected graph
direction, and adds no incoming/outgoing reference counts. Article discovery
and successful publisher-page collection can occur at different times. Neither
links nor discovery order establish copying, influence or causation.

## Future analysis: source novelty and a repost index

A future analysis view could compare how often communities or feeds introduce
articles that DANK has not seen, and how quickly they reference already known
articles. Useful candidates include:

- The share of links with independent discovery before their post's known
  publication time, matching the eye indicator.
- Separately, the share already observed elsewhere before DANK collected this
  source's link: a possible **repost index** based on collection order. These
  two comparisons must not be combined; only the first establishes that DANK
  already knew the destination when the linking post was published.
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
