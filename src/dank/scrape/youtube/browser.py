"""Browser interactions stay on listings: never open a video or post detail."""

from __future__ import annotations

import asyncio
import json

import zendriver
from zendriver import cdp

from .payloads import as_dict

SNAPSHOT_SCRIPT = """JSON.stringify((() => {
    const renderers = {
        'ytd-video-renderer': 'videoRenderer',
        'ytd-rich-grid-media': 'videoRenderer',
        'ytd-rich-item-renderer': 'richItemRenderer',
        'ytd-grid-video-renderer': 'gridVideoRenderer',
        'ytd-playlist-video-renderer': 'playlistVideoRenderer',
        'ytd-reel-item-renderer': 'reelItemRenderer',
        'ytd-post-renderer': 'postRenderer',
        'ytd-backstage-post-renderer': 'backstagePostRenderer',
        'yt-lockup-view-model': 'lockupViewModel',
        'yt-shorts-lockup-view-model': 'shortsLockupViewModel',
    };
    const selectors = Object.keys(renderers).join(',');
    const shelfSelector = 'ytd-rich-shelf-renderer,ytd-shelf-renderer,' +
        'ytd-reel-shelf-renderer';
    const items = Array.from(document.querySelectorAll(selectors))
        .filter(node => !node.closest('ytd-ad-slot-renderer,' +
            'ytd-in-feed-ad-layout-renderer'))
        .map(node => {
            const shelves = [];
            const shelfContexts = [];
            let playable = false;
            for (let parent = node.parentElement; parent;
                    parent = parent.parentElement) {
                playable ||= /playable|mini-game/i.test(parent.tagName);
                if (!parent.matches(shelfSelector)) continue;
                const context = parent.data || parent.__data?.data || {};
                const heading = parent.querySelector('#title')
                    ?.textContent?.trim() || '';
                shelves.push(heading);
                shelfContexts.push({title: context.title || heading,
                    endpoint: context.endpoint});
            }
            const data = node.data || node.__data?.data;
            return {value: {[renderers[node.tagName.toLowerCase()]]: data},
                shelf: shelves[0] || '', shelves,
                shelf_contexts: shelfContexts, playable};
        }).filter(item => Object.values(item.value)[0]);
    const root = document.scrollingElement || document.documentElement;
    return {url: location.href, text: document.body?.innerText || '',
        at_bottom: root.scrollTop + window.innerHeight
            >= root.scrollHeight - 100,
        scroll_height: root.scrollHeight,
        loading: Array.from(document.querySelectorAll(
            'ytd-continuation-item-renderer tp-yt-paper-spinner,' +
            'ytd-continuation-item-renderer tp-yt-paper-spinner-lite'
        )).some(node => node.active && node.getClientRects().length),
        continuation: Array.from(document.querySelectorAll(
            'ytd-continuation-item-renderer'
        )).some(node => node.getClientRects().length),
        initial: window.ytInitialData || {}, items,
        signed_in: window.ytcfg?.get?.('LOGGED_IN') === true};
})())"""
EXPAND_SCRIPT = """(() => {
    let count = 0;
    const selector = 'ytd-rich-shelf-renderer,ytd-shelf-renderer';
    for (const shelf of document.querySelectorAll(selector)) {
        const heading = shelf.querySelector('#title')?.textContent || '';
        if (/^live (now|news)/i.test(heading)) continue;
        const buttons = shelf.querySelectorAll('button,tp-yt-paper-button');
        for (const button of buttons) {
            if (count >= 20) return count;
            if (/^show more$/i.test(button.textContent.trim()) &&
                button.getClientRects().length &&
                !button.disabled && !button.dataset.dankExpanded) {
                button.dataset.dankExpanded = 'true'; button.click(); count++;
            }
        }
    }
    return count;
})()"""
CONSENT_SCRIPT = """(() => {
    for (const button of document.querySelectorAll('button')) {
        if (/^reject all$/i.test(button.textContent.trim())) {
            button.click(); return true;
        }
    }
    return false;
})()"""
SCROLL_SCRIPT = (
    """window.scrollBy(0, Math.max(600, window.innerHeight * 1.5))"""
)

# Visit the bottom of each loaded batch to trigger the next continuation.
# The separate pause adds pacing variation, not a change of identity.
HOME_SCROLL_SCRIPT = """window.scrollTo(0,
    (document.scrollingElement || document.documentElement).scrollHeight)"""


async def navigate(page: zendriver.Tab, url: str) -> None:
    async with asyncio.timeout(30):
        await page.send(cdp.page.navigate(url))

        try:
            await page.wait_for_ready_state(timeout=5)
        except TimeoutError:
            pass

        await asyncio.sleep(2)

        if await page.evaluate(CONSENT_SCRIPT):
            await asyncio.sleep(2)


async def snapshot(page: zendriver.Tab) -> dict[str, object]:
    async with asyncio.timeout(15):
        return as_dict(json.loads(str(await page.evaluate(SNAPSHOT_SCRIPT))))
