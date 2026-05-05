import { Slot } from "@radix-ui/react-slot";
import type { ButtonHTMLAttributes, ReactNode } from "react";

import { cn } from "../../lib/utils";

type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement> & {
  asChild?: boolean;
  variant?: "default" | "primary";
  icon?: ReactNode;
};

export function Button({ asChild, className, variant = "default", icon, children, ...props }: ButtonProps) {
  const Component = asChild ? Slot : "button";

  return (
    <Component className={cn("button", variant === "primary" && "primary", className)} {...props}>
      {icon}
      {children}
    </Component>
  );
}
