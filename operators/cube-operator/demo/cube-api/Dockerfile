FROM node:24.18.0-trixie-slim

WORKDIR /cube
ENV NODE_ENV=production

COPY package.json ./
COPY packages ./packages
COPY rust ./rust
COPY node_modules ./node_modules

COPY cube.js /cube/conf/cube.js
COPY schema /cube/conf/schema

ENV NODE_PATH=/cube/conf/node_modules:/cube/node_modules
ENV CUBEJS_SCHEMA_PATH=schema
WORKDIR /cube/conf

EXPOSE 4000

CMD ["node", "/cube/node_modules/@cubejs-backend/server/bin/server"]
