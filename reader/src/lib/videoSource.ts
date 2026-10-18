/** Compare publishing sites, treating their media and player hosts as the same source. */
export function videoSource(value: string | null | undefined) {
  if (!value) return undefined;

  try {
    const url = new URL(value);

    if (
      !["http:", "https:"].includes(url.protocol) ||
      url.username ||
      url.password
    )
      return undefined;

    const host = url.hostname.replace(/^www\./, "");
    const sites = [
      { name: "Reddit", domains: ["reddit.com", "redd.it", "redditmedia.com"] },
      { name: "X.com", domains: ["x.com", "twitter.com", "twimg.com"] },
      {
        name: "YouTube",
        domains: [
          "youtube.com",
          "youtu.be",
          "youtube-nocookie.com",
          "googlevideo.com",
        ],
      },
    ];
    const site = sites.find(({ domains }) =>
      domains.some((domain) => host === domain || host.endsWith(`.${domain}`)),
    );

    return { name: site?.name ?? host, url: url.href };
  } catch {
    return undefined;
  }
}
