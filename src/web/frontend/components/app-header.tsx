"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { cx } from "@/components/ui";
import { getModelSettings, setModelSettings, type ModelSettingsResponse } from "@/lib/api";

const NAV_ITEMS: Array<{ href: "/" | "/rules" | "/stats"; label: string; match: (pathname: string) => boolean }> = [
  { href: "/", label: "任务控制台", match: (pathname: string) => pathname === "/" || pathname.startsWith("/jobs/") },
  { href: "/rules", label: "规则工作台", match: (pathname: string) => pathname.startsWith("/rules") },
  { href: "/stats", label: "统计分析", match: (pathname: string) => pathname.startsWith("/stats") },
];

function getPageHint(pathname: string) {
  if (pathname.startsWith("/stats")) {
    return "查看任务耗时、步骤耗时、抽取规模与字段频率热力图";
  }
  if (pathname.startsWith("/rules")) {
    return "编辑、校验并发布 field_rules 版本";
  }
  if (pathname.startsWith("/jobs/")) {
    return "查看任务执行时间线、SSE 事件与最终结果";
  }
  return "创建任务、上传投标文件并启动匹配流程";
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
          <Link href="/" className="app-brand-title">
            Suisse Bid Match
          </Link>
          <p className="app-brand-subtitle">{getPageHint(pathname)}</p>
        </div>
        <nav className="app-nav" aria-label="主导航">
          {NAV_ITEMS.map((item) => {
            const active = item.match(pathname);
            return (
              <Link key={item.href} href={item.href} className={cx("app-nav-link", active && "app-nav-link-active")}>
                {item.label}
              </Link>
            );
          })}
          <label className="app-model-control" htmlFor="global-model-select">
            <span className="app-model-label">模型</span>
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
            {modelSettings?.has_api_key ? "API Key 已配置" : "API Key 缺失"}
          </span>
        </nav>
      </div>
    </header>
  );
}
