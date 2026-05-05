import type { CSSProperties, ReactNode } from "react";

type Tone = "neutral" | "success" | "warning" | "danger" | "info" | "accent";

export function Pill({
  tone = "neutral",
  children,
  icon,
  className
}: {
  tone?: Tone;
  children: ReactNode;
  icon?: ReactNode;
  className?: string;
}) {
  return (
    <span className={["cc-pill", `cc-pill-${tone}`, className].filter(Boolean).join(" ")}>
      {icon ? <span className="cc-pill-icon">{icon}</span> : null}
      <span>{children}</span>
    </span>
  );
}

export function Dot({ tone = "neutral", size = 7 }: { tone?: Tone; size?: number }) {
  const style: CSSProperties = { width: size, height: size };
  return <span className={`cc-dot cc-dot-${tone}`} style={style} aria-hidden="true" />;
}

export function HelpBadge({ tip, label }: { tip: string; label?: string }) {
  return (
    <span className="help-badge tooltip-host" tabIndex={0} aria-label={label ?? "Help"} role="button">
      ?
      <span role="tooltip" className="tooltip">
        {tip}
      </span>
    </span>
  );
}
