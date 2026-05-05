import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { createSourceConfig, listSourceConfigs, updateSourceConfig } from "./source-configs";
import type { SourceConfigRequest } from "./types";

export const sourceConfigsQueryKey = ["source-configs"] as const;

export function useSourceConfigs() {
  return useQuery({
    queryKey: sourceConfigsQueryKey,
    queryFn: listSourceConfigs
  });
}

export function useCreateSourceConfig() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (config: SourceConfigRequest) => createSourceConfig(config),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: sourceConfigsQueryKey });
    }
  });
}

export function useUpdateSourceConfig() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (config: SourceConfigRequest) => updateSourceConfig(config),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: sourceConfigsQueryKey });
    }
  });
}
