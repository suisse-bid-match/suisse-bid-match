"use client";

import { useEffect, useState } from "react";
import Image from "next/image";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { cx } from "@/components/ui";
import { getModelSettings, setModelSettings, type ModelSettingsResponse } from "@/lib/api";
import TenderLogo from "../../../../assets/TenderLogo.jpeg";

const NAV_ITEMS: Array<{ href: "/" | "/rules" | "/stats"; label: string; match: (pathname: string) => boolean }> = [
  { href: "/", label: "Task Console", match: (pathname: string) => pathname === "/" || pathname.startsWith("/jobs/") },
  { href: "/rules", label: "Rules Workbench", match: (pathname: string) => pathname.startsWith("/rules") },
  { href: "/stats", label: "Analytics", match: (pathname: string) => pathname.startsWith("/stats") },
];

function getPageHint(pathname: string) {
  if (pathname.startsWith("/stats")) {
    return "View job duration, step duration, extraction scale, and field frequency heatmap";
  }
  if (pathname.startsWith("/rules")) {
    return "Edit, validate, and publish field_rules versions";
  }
  if (pathname.startsWith("/jobs/")) {
    return "View the execution timeline, SSE events, and final results";
  }
  return "Create jobs, upload tender files, and start the matching pipeline";
}

export function AppHeader() {
  const pathname = usePathname();
  const [modelSettings, setModelSettingsState] = useState<ModelSettingsResponse | null>(null);
  const [updatingModel, setUpdatingModel] = useState(false);

  useEffect(() => {
    void getModelSettings()
      .then(setModelSettingsState)
      .catch(() => null);
  }, []);

  async function handleModelChange(nextModel: "gpt-5.4" | "gpt-5-mini") {
    setUpdatingModel(true);
    try {
      const payload = await setModelSettings(nextModel);
      setModelSettingsState(payload);
    } catch {
      // keep previous selection and let critical actions fail with explicit backend error
    } finally {
      setUpdatingModel(false);
    }
  }

  return (
    <header className="app-header">
      <div className="page-wrap app-header-inner">
        <div className="app-brand">
          <Link href="/" className="app-brand-link" aria-label="Heidi Tender home">
            <span className="app-brand-mark">
              <Image
                src={TenderLogo}
                alt="Heidi Tender logo"
                className="app-brand-logo"
                priority
                sizes="52px"
              />
            </span>
            <span className="app-brand-copy">
              <span className="app-brand-title">Heidi Tender</span>
              <span className="app-brand-subtitle">{getPageHint(pathname)}</span>
            </span>
          </Link>
        </div>
        <nav className="app-nav" aria-label="Main navigation">
          {NAV_ITEMS.map((item) => {
            const active = item.match(pathname);
            return (
              <Link key={item.href} href={item.href} className={cx("app-nav-link", active && "app-nav-link-active")}>
                {item.label}
              </Link>
            );
          })}
          <label className="app-model-control" htmlFor="global-model-select">
            <span className="app-model-label">Model</span>
            <select
              id="global-model-select"
              className="app-model-select"
              value={modelSettings?.current_model ?? "gpt-5-mini"}
              onChange={(event) => handleModelChange(event.target.value as "gpt-5.4" | "gpt-5-mini")}
              disabled={updatingModel}
            >
              <option value="gpt-5.4">gpt-5.4</option>
              <option value="gpt-5-mini">gpt-5-mini</option>
            </select>
          </label>
          <span className={cx("app-key-state", modelSettings?.has_api_key ? "app-key-state-on" : "app-key-state-off")}>
            {modelSettings?.has_api_key ? "API key configured" : "API key missing"}
          </span>
        </nav>
      </div>
    </header>
  );
}
