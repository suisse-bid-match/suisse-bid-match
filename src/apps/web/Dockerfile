FROM node:20-alpine

WORKDIR /app/apps/web

COPY apps/web/package*.json ./
RUN npm install

COPY apps/web ./

EXPOSE 3000

CMD ["npm", "run", "dev", "--", "-H", "0.0.0.0", "-p", "3000"]
