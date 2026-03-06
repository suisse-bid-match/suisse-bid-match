import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        ink: "#0f172a",
        paper: "#f8fafc",
        accent: "#0f766e",
        accentSoft: "#99f6e4",
        warm: "#f59e0b"
      },
      boxShadow: {
        soft: "0 18px 45px -30px rgba(2, 6, 23, 0.5)"
      }
    },
  },
  plugins: [],
};

export default config;
