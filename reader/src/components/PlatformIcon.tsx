// Tabler Icons (MIT): https://github.com/tabler/tabler-icons/tree/main/icons/outline
// License: /reader/tabler-icons-license.txt
export function PlatformIcon({
  platform,
  size = 19,
}: {
  platform: "x" | "reddit";
  size?: number;
}) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={1.75}
      strokeLinecap="round"
      strokeLinejoin="round"
      className="platform-icon"
      aria-hidden="true"
      focusable="false"
    >
      {platform === "x" ? (
        <>
          <path d="M4 4l11.733 16h4.267l-11.733 -16l-4.267 0" />
          <path d="M4 20l6.768 -6.768m2.46 -2.46l6.772 -6.772" />
        </>
      ) : (
        <>
          <path d="M12 8c2.648 0 5.028 .826 6.675 2.14a2.5 2.5 0 0 1 2.326 4.36c0 3.59 -4.03 6.5 -9 6.5c-4.875 0 -8.845 -2.8 -9 -6.294l-1 -.206a2.5 2.5 0 0 1 2.326 -4.36c1.646 -1.313 4.026 -2.14 6.674 -2.14l.999 0" />
          <path d="M12 8l1 -5l6 1" />
          <path d="M18 4a1 1 0 1 0 2 0a1 1 0 1 0 -2 0" />
          <path
            d="M8.5 13a.5 .5 0 1 0 1 0a.5 .5 0 1 0 -1 0"
            fill="currentColor"
          />
          <path
            d="M14.5 13a.5 .5 0 1 0 1 0a.5 .5 0 1 0 -1 0"
            fill="currentColor"
          />
          <path d="M10 17c.667 .333 1.333 .5 2 .5s1.333 -.167 2 -.5" />
        </>
      )}
    </svg>
  );
}
