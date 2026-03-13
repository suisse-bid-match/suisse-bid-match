import type { ReactNode } from "react";

export function cx(...items: Array<string | false | null | undefined>) {
  return items.filter(Boolean).join(" ");
}

export type StatusTone = "idle" | "running" | "succeeded" | "failed" | "pending" | "active" | "done" | "error";

const STATUS_TONE_CLASS: Record<StatusTone, string> = {
  idle: "status-idle",
  running: "status-running",
  succeeded: "status-succeeded",
  failed: "status-failed",
  pending: "status-pending",
  active: "status-active",
  done: "status-done",
  error: "status-error"
};

const STATUS_KEYWORD_TO_TONE: Record<string, StatusTone> = {
  idle: "idle",
  created: "idle",
  ready: "active",
  uploading: "running",
  creating: "running",
  starting: "running",
  running: "running",
  ok: "succeeded",
  success: "succeeded",
  succeeded: "succeeded",
  completed: "succeeded",
  done: "done",
  failed: "failed",
  error: "error",
  pending: "pending",
  active: "active"
};

export function toneFromKeyword(input: string | null | undefined): StatusTone {
  const key = (input ?? "").toLowerCase();
  return STATUS_KEYWORD_TO_TONE[key] ?? "idle";
}

interface StatusBadgeProps {
  label: string;
  tone?: StatusTone;
  className?: string;
}

export function StatusBadge({ label, tone = "idle", className }: StatusBadgeProps) {
  return <span className={cx("status-badge", STATUS_TONE_CLASS[tone], className)}>{label}</span>;
}

interface ActionButtonProps {
  type?: "button" | "submit" | "reset";
  onClick?: () => void;
  disabled?: boolean;
  variant?: "primary" | "secondary" | "ghost" | "success" | "danger" | "warning";
  children: ReactNode;
  className?: string;
}

export function ActionButton({
  type = "button",
  onClick,
  disabled,
  variant = "secondary",
  children,
  className
}: ActionButtonProps) {
  return (
    <button
      type={type}
      onClick={onClick}
      disabled={disabled}
      className={cx("btn", `btn-${variant}`, className)}
    >
      {children}
    </button>
  );
}

interface InlineNoticeProps {
  tone?: "info" | "success" | "warning" | "error";
  title?: string;
  message: string;
  className?: string;
}

export function InlineNotice({ tone = "info", title, message, className }: InlineNoticeProps) {
  return (
    <div className={cx("notice", `notice-${tone}`, className)} role="status" aria-live="polite">
      {title ? <strong className="notice-title">{title}</strong> : null}
      <p className="notice-message">{message}</p>
    </div>
  );
}

interface InfoCardProps {
  title: string;
  subtitle?: string;
  value: string;
  tone?: StatusTone;
}

export function InfoCard({ title, subtitle, value, tone = "idle" }: InfoCardProps) {
  return (
    <article className="info-card">
      <div className="info-card-top">
        <span className="info-card-title">{title}</span>
        <StatusBadge label={tone.toUpperCase()} tone={tone} />
      </div>
      <div className="info-card-value">{value}</div>
      {subtitle ? <p className="info-card-subtitle">{subtitle}</p> : null}
    </article>
  );
}

interface SectionHeaderProps {
  title: string;
  subtitle?: string;
  right?: ReactNode;
}

export function SectionHeader({ title, subtitle, right }: SectionHeaderProps) {
  return (
    <div className="section-header">
      <div>
        <h2 className="section-title">{title}</h2>
        {subtitle ? <p className="section-subtitle">{subtitle}</p> : null}
      </div>
      {right ? <div className="section-header-right">{right}</div> : null}
    </div>
  );
}

interface EmptyStateProps {
  title: string;
  description: string;
}

export function EmptyState({ title, description }: EmptyStateProps) {
  return (
    <div className="empty-state">
      <p className="empty-state-title">{title}</p>
      <p className="empty-state-description">{description}</p>
    </div>
  );
}
