FROM node:20-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY index.js ./
ENV PORT=8000
EXPOSE 8000
CMD ["node", "index.js"]
