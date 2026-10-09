import { zodResolver } from "@hookform/resolvers/zod";
import { useState } from "react";
import { useForm, useWatch } from "react-hook-form";
import { useNavigate } from "react-router";
import { z } from "zod";

import { useEnqueue } from "../api/queries";
import type { NewJob } from "../api/types";
import { ErrorNote } from "../components/States";

function parses(kind: "array" | "object") {
  return (text: string) => {
    try {
      const value: unknown = JSON.parse(text);
      return kind === "array" ? Array.isArray(value) : typeof value === "object" && value !== null && !Array.isArray(value);
    } catch {
      return false;
    }
  };
}

export const jobFormSchema = z
  .object({
    task: z.string().trim().regex(/^[A-Za-z_][A-Za-z0-9_.:-]{0,199}$/, "Use letters, digits, _ . : - (e.g. examples.echo)"),
    queue: z.string().trim().regex(/^[A-Za-z0-9_.:-]{1,64}$/, "1-64 of letters, digits, _ . : -"),
    args: z.string().refine(parses("array"), "Must be a JSON array, e.g. [1, \"a\"]"),
    kwargs: z.string().refine(parses("object"), "Must be a JSON object, e.g. {\"n\": 30}"),
    when: z.enum(["now", "delay", "at"]),
    delay_s: z.coerce.number<string>().min(0).max(365 * 86400),
    run_at: z.string(),
    max_attempts: z.coerce.number<string>().int().min(1).max(100),
    timeout_s: z.coerce.number<string>().gt(0).max(86400),
    idempotency_key: z.string().trim().max(255),
  })
  .refine((v) => v.when !== "at" || !Number.isNaN(Date.parse(v.run_at)), {
    path: ["run_at"],
    message: "Pick a date and time",
  });

type FormInput = z.input<typeof jobFormSchema>;
type FormOutput = z.output<typeof jobFormSchema>;

export function toNewJob(v: FormOutput): NewJob {
  return {
    task: v.task,
    queue: v.queue,
    args: JSON.parse(v.args) as unknown[],
    kwargs: JSON.parse(v.kwargs) as Record<string, unknown>,
    max_attempts: v.max_attempts,
    timeout_s: v.timeout_s,
    ...(v.when === "delay" ? { delay_s: v.delay_s } : {}),
    ...(v.when === "at" ? { run_at: new Date(v.run_at).toISOString() } : {}),
  };
}

const PRESETS: { label: string; values: Partial<FormInput> }[] = [
  { label: "Echo", values: { task: "examples.echo", args: '["hello"]', kwargs: '{"from": "dashboard"}', max_attempts: "3" } },
  { label: "Sleep 5 s", values: { task: "examples.sleep", args: "[5]", kwargs: "{}", max_attempts: "3" } },
  { label: "Flaky (retries)", values: { task: "examples.flaky", args: "[]", kwargs: '{"p_fail": 0.7}', max_attempts: "5" } },
  { label: "Always fails (dead letter)", values: { task: "examples.fail", args: "[]", kwargs: "{}", max_attempts: "1" } },
];

const freshKey = () => crypto.randomUUID();

export function NewJobPage() {
  const navigate = useNavigate();
  const enqueue = useEnqueue();
  const [defaults] = useState<FormInput>(() => ({
    task: "examples.echo",
    queue: "default",
    args: '["hello"]',
    kwargs: "{}",
    when: "now",
    delay_s: "10",
    run_at: "",
    max_attempts: "3",
    timeout_s: "300",
    idempotency_key: freshKey(),
  }));
  const {
    register,
    handleSubmit,
    reset,
    getValues,
    control,
    formState: { errors },
  } = useForm<FormInput, unknown, FormOutput>({ resolver: zodResolver(jobFormSchema), defaultValues: defaults });
  const when = useWatch({ control, name: "when" });

  const onSubmit = handleSubmit((values) =>
    enqueue.mutate(
      { job: toNewJob(values), idempotencyKey: values.idempotency_key || undefined },
      { onSuccess: (job) => void navigate(`/jobs/${job.id}`) },
    ),
  );

  const error = (name: keyof FormInput) =>
    errors[name] && <span role="alert" className="text-xs text-red-600">{errors[name]?.message}</span>;

  return (
    <div className="max-w-3xl space-y-4">
      <h1 className="text-2xl font-bold tracking-tight">New job</h1>
      <div className="flex flex-wrap gap-2" aria-label="Presets">
        {PRESETS.map((p) => (
          <button key={p.label} type="button" className="btn btn-secondary" onClick={() => reset({ ...getValues(), ...p.values, idempotency_key: freshKey() })}>
            {p.label}
          </button>
        ))}
      </div>
      <form onSubmit={onSubmit} className="card space-y-4" noValidate aria-label="New job">
        <div className="grid gap-4 sm:grid-cols-2">
          <label className="space-y-1 text-sm"><span className="block font-medium">Task</span><input className="field font-mono" {...register("task")} />{error("task")}</label>
          <label className="space-y-1 text-sm"><span className="block font-medium">Queue</span><input className="field" {...register("queue")} />{error("queue")}</label>
          <label className="space-y-1 text-sm"><span className="block font-medium">args (JSON array)</span><textarea rows={3} className="field font-mono" {...register("args")} />{error("args")}</label>
          <label className="space-y-1 text-sm"><span className="block font-medium">kwargs (JSON object)</span><textarea rows={3} className="field font-mono" {...register("kwargs")} />{error("kwargs")}</label>
        </div>
        <fieldset className="space-y-2">
          <legend className="text-sm font-medium">When</legend>
          <div className="flex flex-wrap gap-4 text-sm">
            {(["now", "delay", "at"] as const).map((w) => (
              <label key={w} className="flex items-center gap-1.5">
                <input type="radio" value={w} {...register("when")} />
                {w === "now" ? "Now" : w === "delay" ? "After a delay" : "At a time"}
              </label>
            ))}
          </div>
          {when === "delay" && (
            <label className="flex items-center gap-2 text-sm"><input type="number" min={0} className="field w-32" {...register("delay_s")} /> seconds {error("delay_s")}</label>
          )}
          {when === "at" && (
            <label className="block text-sm"><input type="datetime-local" className="field w-64" aria-label="Run at" {...register("run_at")} /> {error("run_at")}</label>
          )}
        </fieldset>
        <div className="grid gap-4 sm:grid-cols-3">
          <label className="space-y-1 text-sm"><span className="block font-medium">Max attempts</span><input type="number" min={1} max={100} className="field" {...register("max_attempts")} />{error("max_attempts")}</label>
          <label className="space-y-1 text-sm"><span className="block font-medium">Timeout (s)</span><input type="number" min={1} className="field" {...register("timeout_s")} />{error("timeout_s")}</label>
          <label className="space-y-1 text-sm"><span className="block font-medium">Idempotency key</span><input className="field font-mono text-xs" {...register("idempotency_key")} />{error("idempotency_key")}</label>
        </div>
        <p className="text-xs text-slate-500">
          The idempotency key makes a double click or a retried request return the same job instead of a second one.
        </p>
        {enqueue.error && <ErrorNote error={enqueue.error} />}
        <button type="submit" className="btn btn-primary" disabled={enqueue.isPending}>
          {enqueue.isPending ? "Enqueuing…" : "Enqueue job"}
        </button>
      </form>
    </div>
  );
}
