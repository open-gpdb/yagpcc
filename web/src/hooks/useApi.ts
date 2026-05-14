import { useState, useEffect, useCallback, useRef } from "react";

/**
 * Generic hook for fetching data from the API.
 * Returns { data, loading, error, refresh }.
 */
export function useApi<T>(
  fetcher: () => Promise<T>,
  deps: unknown[] = [],
) {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const requestIdRef = useRef(0);

  const refresh = useCallback(() => {
    const requestId = ++requestIdRef.current;
    setLoading(true);
    setError(null);
    fetcher()
      .then((response) => {
        if (requestId === requestIdRef.current) {
          setData(response);
        }
      })
      .catch((err: unknown) => {
        if (requestId === requestIdRef.current) {
          setError(err instanceof Error ? err : new Error(String(err)));
        }
      })
      .finally(() => {
        if (requestId === requestIdRef.current) {
          setLoading(false);
        }
      });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  useEffect(() => {
    refresh();
    return () => {
      requestIdRef.current += 1;
    };
  }, [refresh]);

  return { data, loading, error, refresh };
}
