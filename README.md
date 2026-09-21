# NIFTY Flow Evidence Backend

One Render backend for the mobile PWA. It is a **data provider and evidence engine**, not a trading/execution engine and not a replacement for the user's RSI/DEMA setup.

## Data path
Dhan tick-by-tick feed + Dhan Option Chain + Dhan 20-level depth -> normalized WebSocket/REST -> PWA.

The backend exposes:
- live NIFTY tick/spot data
- option-chain OI, change-OI snapshot, volume, change-volume, premium, IV, Gamma, Theta, bid/ask
- 20-level depth aggregates for the nearest option contracts
- flow evidence: positioning, premium/volume flow, depth imbalance and writing/unwind proxies
- Web Push bridge

The backend deliberately does **not** generate CALL/PUT entries. The PWA owns the RSI5/RSI9 + SMA14 + 50 + DEMA14 sequence.

## Render
Build: `npm install`
Start: `npm start`
Root directory: blank.

Set the variables in `.env.example`. Keep the Dhan PIN/TOTP secret and VAPID private key only on Render.

## Important interpretation rule
OI alone does not prove writing. The backend labels writing/unwind as proxies using changes in OI and premium. The PWA meter also uses breadth, volume, depth, price momentum and technical sequence state.

The meter is intentionally stateful: a normal pullback can reduce strength without immediately flipping CALL to PUT. An opposite direction must persist and overwhelm the existing trend state before the direction flips.
