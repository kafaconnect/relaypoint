export const cmdSubject = (tenantId: string, interactionId: string, selfUserId: string): string =>
  `tenant.${tenantId}.interaction.${interactionId}.cmd.${selfUserId}`;

export const logSubject = (tenantId: string, interactionId: string): string =>
  `tenant.${tenantId}.interaction.${interactionId}.log`;

export const signalSubject = (tenantId: string, interactionId: string, selfUserId: string): string =>
  `tenant.${tenantId}.interaction.${interactionId}.signal.${selfUserId}`;

export const agentFeedSubject = (tenantId: string, selfUserId: string): string =>
  `tenant.${tenantId}.agent.${selfUserId}.feed.>`;

export function interactionIdFromFeedSubject(subject: string | undefined): string | undefined {
  const parts = subject?.split(".");
  if (!parts || parts.length !== 6) return undefined;
  if (parts[0] !== "tenant" || parts[2] !== "agent" || parts[4] !== "feed") return undefined;
  return parts[5] || undefined;
}
