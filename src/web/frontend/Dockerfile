FROM node:20-alpine

WORKDIR /app/src/web/frontend

COPY src/web/frontend/package*.json ./
RUN npm install

COPY src/web/frontend .
COPY assets /app/assets

ENV NEXT_TELEMETRY_DISABLED=1
CMD ["npm", "run", "dev", "--", "-H", "0.0.0.0", "-p", "3000"]
