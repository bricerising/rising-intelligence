FROM node:20-alpine

WORKDIR /app

COPY tests/e2e/mock-llm/server.mjs ./server.mjs

ENV PORT=8080

EXPOSE 8080

CMD ["node", "server.mjs"]
