import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from "react";

export type TopbarIdentity = {
  topic: string;
  sourceEventType: string;
  enabled: boolean;
};

type TopbarIdentityContextValue = {
  identity: TopbarIdentity | null;
  setIdentity: (identity: TopbarIdentity | null) => void;
};

const TopbarIdentityContext = createContext<TopbarIdentityContextValue>({
  identity: null,
  setIdentity: () => undefined
});

export function TopbarIdentityProvider({ children }: { children: ReactNode }) {
  const [identity, setIdentity] = useState<TopbarIdentity | null>(null);
  const value = useMemo(() => ({ identity, setIdentity }), [identity]);
  return <TopbarIdentityContext.Provider value={value}>{children}</TopbarIdentityContext.Provider>;
}

export function useTopbarIdentity() {
  return useContext(TopbarIdentityContext);
}

export function usePublishTopbarIdentity(identity: TopbarIdentity | null) {
  const { setIdentity } = useContext(TopbarIdentityContext);
  useEffect(() => {
    setIdentity(identity);
    return () => setIdentity(null);
  }, [setIdentity, identity?.topic, identity?.sourceEventType, identity?.enabled]);
}
