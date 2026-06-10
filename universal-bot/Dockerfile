FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install --production

COPY bot.js .

CMD ["node", "bot.js"]
