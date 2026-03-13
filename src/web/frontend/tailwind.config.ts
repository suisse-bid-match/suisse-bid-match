import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./app/**/*.{js,ts,jsx,tsx}", "./components/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        panel: "#0f1c24",
        accent: "#f59e0b",
        ink: "#eef4f7"
      },
      boxShadow: {
        glow: "0 20px 70px rgba(245, 158, 11, 0.2)"
      }
    }
  },
  plugins: []
};

export default config;
