"""Read public thread HTML in the browser without running another page."""

from __future__ import annotations

import asyncio
import json
from contextlib import nullcontext
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit

import zendriver

from dank.scrape.http import current_limiter

from .access import check_response_status, current_requests, pace_request
from .payloads import as_dict, number, text

SNAPSHOT_FUNCTION = """(root, url, postId) => {
    const post = root.querySelector('shreddit-post');
    const comments = Array.from(root.querySelectorAll('shreddit-comment'));
    return {
        url,
        html: post?.outerHTML || '',
        comments: comments.map(node => {
            const body = Array.from(node.querySelectorAll('[slot="comment"]'))
                .find(body => body.closest('shreddit-comment') === node);
            const parent = node.parentElement?.closest('shreddit-comment');
            return {
                id: node.getAttribute('thingid') || node.id,
                parent_id: parent?.getAttribute('thingid')
                    || node.getAttribute('parentid') || post?.id || postId,
                author: node.getAttribute('author') || '',
                body_html: body?.innerHTML || '',
                body: body?.innerText || body?.textContent || '',
                score: node.getAttribute('score'),
                created_utc: node.getAttribute('created'),
                permalink: node.getAttribute('permalink'),
                is_op: node.getAttribute('is-op') === 'true',
                depth: node.getAttribute('depth'),
            };
        }),
        comments_html: comments.filter(node =>
            !node.parentElement?.closest('shreddit-comment'))
            .map(node => node.outerHTML).join('\\n'),
        next_cursor: root.querySelector(
            '#top-level-more-comments-partial input[name="cursor"]',
        )?.value || '',
        next_url: root.querySelector(
            '#top-level-more-comments-partial')?.getAttribute('src') || '',
        can_scroll: !!root.querySelector(
            '#top-level-more-comments-partial'),
        has_more: !!root.querySelector(
            'shreddit-comment-tree [slot="more-comments"], '
            + 'shreddit-comment-tree faceplate-partial[src*="comment"]'),
    };
}"""


DETAIL_SNAPSHOT_SCRIPT = (
    f"JSON.stringify(({SNAPSHOT_FUNCTION})(document, location.href, ''))"
)


async def fetch_document(
    page: zendriver.Tab, url: str, post_id: str, *, cursor: str = "",
) -> dict[str, object] | None:
    # Match the page's read-only fragment request and response format.
    partial = urlsplit(url).path.startswith("/svc/shreddit/more-comments/")
    options: dict[str, object] = {"headers": {"Accept": "text/html"}}

    if partial:
        options = {
            "method": "POST", "body": urlencode({"cursor": cursor}),
            "headers": {
                "Accept": "text/vnd.reddit.partial+html, text/html;q=0.9",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        }

    # A blank detail tab is initialized by the normal first post visit.
    script = """(async () => {
        if (location.origin !== 'https://www.reddit.com') return '';
        const response = await fetch(URL_VALUE, {
            ...FETCH_OPTIONS, credentials: 'same-origin',
            signal: AbortSignal.timeout(10000),
        });
        let snapshot = null;
        if (response.ok) {
            const holder = document.createElement('template');
            holder.innerHTML = await response.text();
            const root = holder.content;
            // Template contents have their own document and stay inactive.
            // https://developer.mozilla.org/en-US/docs/Web/API/HTMLTemplateElement/content
            // Reddit streams comments into inert templates. Unwrap without
            // attaching to the live page or executing any captured scripts.
            let template;
            while ((template = root.querySelector('template'))) {
                template.replaceWith(template.content);
            }
            snapshot = (SNAPSHOT_FUNCTION)(root, response.url, POST_ID);
        }
        return JSON.stringify({status: response.status, url: response.url,
            retry_after: response.headers.get('Retry-After'), snapshot});
    })()""".replace("FETCH_OPTIONS", json.dumps(options)).replace(
        "URL_VALUE", json.dumps(url),
    ).replace(
        "POST_ID", json.dumps(post_id),
    ).replace("SNAPSHOT_FUNCTION", SNAPSHOT_FUNCTION)
    limiter = current_limiter.get()

    async with limiter.slot(url) if limiter else nullcontext():
        await pace_request()

        async with asyncio.timeout(15):
            value = await page.evaluate(script, await_promise=True)

    if not value:
        return None

    result = as_dict(json.loads(str(value)))
    status = number(result.get("status")) or 0
    requests = current_requests.get()

    if status in (401, 403):
        if requests is not None and "/comments/" in urlsplit(url).path:
            requests.documents_available = False

        return None

    check_response_status(status, url, text(result.get("retry_after")))

    if not 200 <= status < 300 or result.get("url") != url:
        return None

    return as_dict(result.get("snapshot")) or None


def next_comments_url(
    snapshot: dict[str, object], community: str, post_id: str,
) -> str | None:
    url = urljoin("https://www.reddit.com", text(snapshot.get("next_url")))
    parsed = urlsplit(url)
    expected = f"/svc/shreddit/more-comments/{community}/{post_id}"

    if (
        parsed.scheme != "https" or parsed.netloc != "www.reddit.com"
        or parsed.path.casefold() != expected.casefold()
        or parse_qs(parsed.query).get("top-level") != ["1"]
    ):
        return None

    return url
