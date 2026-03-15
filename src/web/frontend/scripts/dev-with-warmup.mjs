#!/usr/bin/env node

import { spawn } from "node:child_process";
import http from "node:http";
import path from "node:path";
import process from "node:process";

const DEFAULT_PORT = "3000";
const WARMUP_ROUTES = ["/", "/rules", "/stats"];

function readArgValue(args, shortFlag, longFlag) {
  for (let index = args.length - 2; index >= 0; index -= 1) {
    if ((args[index] === shortFlag || args[index] === longFlag) && args[index + 1]) {
      return args[index + 1];
    }
  }
  return null;
}

function waitForRoute(url) {
  return new Promise((resolve) => {
    const request = http.request(
      url,
      {
        method: "HEAD",
        timeout: 120000,
      },
      (response) => {
        response.resume();
        response.on("end", resolve);
      }
    );

    request.on("timeout", () => {
      request.destroy(new Error("timeout"));
    });

    request.on("error", (error) => {
      process.stderr.write(`[warmup] ${url} failed: ${error.message}\n`);
      resolve();
    });

    request.end();
  });
}

async function warmRoutes(port) {
  if (process.env.FRONTEND_SKIP_WARMUP === "1") {
    process.stdout.write("[warmup] skipped by FRONTEND_SKIP_WARMUP=1\n");
    return;
  }

  const baseUrl = `http://127.0.0.1:${port}`;
  for (const route of WARMUP_ROUTES) {
    await waitForRoute(`${baseUrl}${route}`);
  }
  process.stdout.write(`[warmup] completed for ${WARMUP_ROUTES.join(", ")}\n`);
}

const forwardedArgs = process.argv.slice(2);
const port = readArgValue(forwardedArgs, "-p", "--port") ?? DEFAULT_PORT;
const nextBin = path.resolve(
  process.cwd(),
  "node_modules",
  ".bin",
  process.platform === "win32" ? "next.cmd" : "next"
);
const nextArgs = ["dev", ...(forwardedArgs.length > 0 ? forwardedArgs : ["-p", DEFAULT_PORT])];

const child = spawn(nextBin, nextArgs, {
  cwd: process.cwd(),
  env: process.env,
  stdio: ["inherit", "pipe", "pipe"],
});

let warmupStarted = false;

function forwardOutput(stream, target) {
  stream.on("data", (chunk) => {
    const text = chunk.toString();
    target.write(text);

    if (!warmupStarted && text.includes("Ready in")) {
      warmupStarted = true;
      setTimeout(() => {
        void warmRoutes(port);
      }, 250);
    }
  });
}

forwardOutput(child.stdout, process.stdout);
forwardOutput(child.stderr, process.stderr);

child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 1);
});
