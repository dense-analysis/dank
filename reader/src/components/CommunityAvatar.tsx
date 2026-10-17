import { IdentityAvatar } from "./IdentityAvatar";

export function CommunityAvatar({
  community,
  iconUrl,
}: {
  community: string;
  iconUrl?: string | null;
}) {
  return <IdentityAvatar name={community} iconUrl={iconUrl} kind="community" />;
}
