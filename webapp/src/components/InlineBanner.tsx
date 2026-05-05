type InlineBannerProps = {
  title?: string;
  children: string;
  tone?: "warning" | "error";
};

export function InlineBanner({ title, children, tone = "warning" }: InlineBannerProps) {
  return (
    <div className={`inline-banner ${tone === "error" ? "error" : ""}`} role={tone === "error" ? "alert" : "status"}>
      {title ? <strong>{title}: </strong> : null}
      <span>{children}</span>
    </div>
  );
}
