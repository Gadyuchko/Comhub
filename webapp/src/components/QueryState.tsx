import { InlineBanner } from "./InlineBanner";

type QueryStateProps = {
  isLoading?: boolean;
  isRefetching?: boolean;
  isStale?: boolean;
  isError?: boolean;
  errorMessage?: string;
};

export function QueryState({ isLoading, isRefetching, isStale, isError, errorMessage }: QueryStateProps) {
  if (isLoading) {
    return <InlineBanner>Loading source configuration state.</InlineBanner>;
  }

  if (isError) {
    return <InlineBanner tone="error" title="Source configs unavailable">{errorMessage ?? "Request failed."}</InlineBanner>;
  }

  if (isRefetching) {
    return <InlineBanner>Refreshing source configuration state.</InlineBanner>;
  }

  if (isStale) {
    return <InlineBanner>Source configuration state is stale and will refresh on the next check.</InlineBanner>;
  }

  return null;
}
