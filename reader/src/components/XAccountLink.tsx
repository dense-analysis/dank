import { filterParams } from "../lib/navigation";
import { defaultFilters } from "../lib/storage";
import type { XProfile } from "../lib/types";
import { IdentityAvatar } from "./IdentityAvatar";
import { ReaderLink } from "./ReaderLink";

export function XAccountLink({
  account,
  select,
  showName = false,
}: {
  account: XProfile;
  select: (handle: string) => void;
  showName?: boolean;
}) {
  return (
    <ReaderLink
      className={`source-link x-account-link ${showName ? "x-account-identity" : ""}`}
      href={`/reader/?${filterParams({ ...defaultFilters, kind: "x", account: account.handle })}`}
      onNavigate={() => select(account.handle)}
      aria-label={`Posts from @${account.handle}`}
      title={`${account.name} (@${account.handle})`}
    >
      <IdentityAvatar
        name={account.handle}
        iconUrl={account.icon_url}
        kind="account"
      />
      <span className="x-account-labels">
        {showName && (
          <strong className="account-name">
            {account.name || account.handle}
          </strong>
        )}
        <span className="account-handle">@{account.handle}</span>
      </span>
    </ReaderLink>
  );
}
