# Real-Time Crypto Market Pipeline

Live cryptocurrency market data captured from CoinGecko, carried through two separate streams, stored, and displayed. This glossary fixes the words used in the code, the documentation and the video.

## Records

**Coin Price**:
One coin's price and 24-hour figures, as captured at one Capture Time.
_Avoid_: Market price, price row, coin row, stored record

**Market Snapshot**:
The whole crypto market's totals (market cap, volume, dominance), as captured at one Capture Time.
_Avoid_: Global stats, whole market, market totals

## Time

**Capture Time**:
The moment a producer captured a Coin Price or Market Snapshot. It is the time used to match one record across every stage of the pipeline, and it is always shown in UTC.
_Avoid_: Timestamp, captured time, local time

**Capture Day**:
The UTC calendar day of a record's Capture Time; the day the record is filed under. Before 08:00 in UTC+8, the Capture Day is still the previous date.
_Avoid_: Date, today

**Source Update Time**:
The moment CoinGecko last refreshed the values in a record. It is earlier than, and separate from, the Capture Time. Stored as `last_updated` for Coin Prices and `updated_at` for Market Snapshots.
_Avoid_: Last updated, updated at

**Processing Time**:
The moment Spark processed a record on its way to storage.
_Avoid_: Processed at, write time, store time

## Derived values

**Trend**:
The direction of a coin's 24-hour price change as reported by CoinGecko: UP, DOWN or FLAT. It is not a comparison between two captures. A coin with no reported 24-hour change has no Trend.
_Avoid_: Momentum, price movement

**Volatility (24h)**:
How wide a coin's 24-hour price range is: (24h high − 24h low) ÷ 24h low, as a percentage.
_Avoid_: Risk, spread

**Market Sentiment**:
The mood of a Market Snapshot, judged from the total market cap's 24-hour change: BULLISH at +1% or more, BEARISH at −1% or less, otherwise NEUTRAL.
_Avoid_: Mood, outlook
