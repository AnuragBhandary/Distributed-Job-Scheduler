import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { z } from "zod";

import { ApiError, http } from "../api/http";
import { session } from "../api/session";

const schema = z.object({
  key: z
    .string()
    .trim()
    .regex(/^jq_[0-9a-f]{12}_.+$/, "An API key looks like jq_<12 hex characters>_<secret>"),
});

export function SignInPage() {
  const {
    register,
    handleSubmit,
    setError,
    formState: { errors, isSubmitting },
  } = useForm({ resolver: zodResolver(schema), defaultValues: { key: "" } });

  const onSubmit = handleSubmit(async ({ key }) => {
    try {
      await http.get("/v1/stats", key); // proves the key works before keeping it
      session.signIn(key);
    } catch (error) {
      const message =
        error instanceof ApiError && error.status === 401 ? "That key was rejected." : (error as Error).message;
      setError("key", { message });
    }
  });

  const reason = session.reason();
  return (
    <main className="grid min-h-dvh place-items-center px-4">
      <form onSubmit={onSubmit} className="card w-full max-w-md space-y-4" noValidate>
        <div>
          <h1 className="text-xl font-bold">jobq dashboard</h1>
          <p className="text-sm text-slate-500">Sign in with an API key. Create one with <code>jobq create-key</code>.</p>
        </div>
        {reason && (
          <p role="status" className="rounded-lg bg-amber-50 px-3 py-2 text-sm text-amber-800 dark:bg-amber-950 dark:text-amber-200">
            {reason}
          </p>
        )}
        <label className="block space-y-1">
          <span className="text-sm font-medium">API key</span>
          <input
            type="password"
            autoComplete="off"
            className="field font-mono"
            aria-invalid={errors.key ? true : undefined}
            {...register("key")}
          />
          {errors.key && <span role="alert" className="text-sm text-red-600">{errors.key.message}</span>}
        </label>
        <button type="submit" className="btn btn-primary w-full" disabled={isSubmitting}>
          {isSubmitting ? "Checking…" : "Sign in"}
        </button>
      </form>
    </main>
  );
}
