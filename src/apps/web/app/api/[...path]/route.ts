import { NextRequest } from "next/server";

function buildUpstreamUrl(path: string[], req: NextRequest): string {
  const base = process.env.API_INTERNAL_BASE_URL ?? "http://localhost:8000";
  const cleanBase = base.replace(/\/$/, "");
  const query = req.nextUrl.searchParams.toString();
  const suffix = query ? `?${query}` : "";
  return `${cleanBase}/${path.join("/")}${suffix}`;
}

async function proxy(method: "GET" | "POST", req: NextRequest, path: string[]) {
  const upstreamUrl = buildUpstreamUrl(path, req);
  const headers: Record<string, string> = {};
  const contentType = req.headers.get("content-type");
  if (contentType) {
    headers["content-type"] = contentType;
  }

  const init: RequestInit = { method, headers };
  if (method !== "GET") {
    init.body = await req.text();
  }

  const upstreamResp = await fetch(upstreamUrl, init);
  const text = await upstreamResp.text();
  const respContentType = upstreamResp.headers.get("content-type") ?? "application/json";

  return new Response(text, {
    status: upstreamResp.status,
    headers: {
      "content-type": respContentType,
    },
  });
}

export async function GET(req: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  const { path } = await context.params;
  return proxy("GET", req, path);
}

export async function POST(req: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  const { path } = await context.params;
  return proxy("POST", req, path);
}
