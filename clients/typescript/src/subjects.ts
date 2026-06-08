// signaling-core subjects, prefixed `tenant.<tenantId>.` (verbatim from the server contract).

export const cmdSubject = (tenantId: string, interactionId: string): string =>
  `tenant.${tenantId}.interaction.${interactionId}.cmd`;

export const logSubject = (tenantId: string, interactionId: string): string =>
  `tenant.${tenantId}.interaction.${interactionId}.log`;

// Own-author signal only — the SDK never writes another user's signal subject.
export const signalSubject = (tenantId: string, interactionId: string, selfUserId: string): string =>
  `tenant.${tenantId}.interaction.${interactionId}.signal.${selfUserId}`;
