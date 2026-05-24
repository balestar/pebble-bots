# Pebble Bots

TCNDelegation sweep bots for ETH, BNB and Polygon chains.

## Structure
- `eth-bot/` — Ethereum mainnet bot
- `bnb-bot/` — BNB Chain bot  
- `polygon-bot/` — Polygon bot

Each bot uses the same `bot.js` code — only the `.env` differs.

## Setup

1. Copy `.env.example` to `.env` in each folder
2. Fill in your private key and Supabase service role key
3. Run with Docker Compose:

```bash
docker-compose up -d
```

## Update bots on Pi

```bash
ssh rainbow@192.168.0.169
cd ~/pebble
git pull
docker-compose restart
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| PRIVATE_KEY | Relayer wallet private key |
| RPC_URL | Alchemy WebSocket RPC URL |
| CONTRACT_ADDRESS | TCNDelegation contract address |
| DESTINATION_ADDRESS | Where swept funds go |
| TOKENS_TO_WATCH | Comma separated token addresses |
| SUPABASE_URL | Supabase project URL |
| SUPABASE_SERVICE_ROLE_KEY | Supabase service role key |
| CHAIN | eth, bnb or polygon |
