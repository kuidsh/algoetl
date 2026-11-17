# Cost and launch plan: algoetl via an Algorand xGov retroactive grant

## Starting investment (non-AWS)
| item | ALGO | ≈ US$ at $0.10/ALGO | refundable? |
|---|---|---|---|
| Proposer fee | 100 | 10 | no |
| Open-proposal fee | 100 | 10 | no |
| Proposal commitment, 3% of ask (35,000 ALGO ask) | 1,050 | 105 | **yes**, returned after the vote unless T&C are breached |
| Network fees | <1 | <0.10 | no |
| **Total outlay** | **≈1,251** | **≈ US$125** | ≈ US$20 sunk |

The fees and 3% commitment come from the xGov configuration page (docs.xgov.algorand.co,
read 2026-11-16). The ALGO price is ≈ US$0.10 (CoinGecko via Coin Gabbar, 2026-11-05) and is
volatile. Buying ALGO through a Mexican exchange is a user step.

## AWS per month (us-east-1)
| service | sizing | cost |
|---|---|---|
| Lambda | 8,640 ticks × ~35 s × 0.5 GB ≈ 151k GB-s; 8,640 requests | $0 inside the free tier (400k GB-s and 1M requests a month); $2.52 at $0.0000166667/GB-s without it |
| S3 Standard | ~5 GB added a month (estimate: ~2M rows/day ≈ 150 MB/day zstd) | ≈ $0.12 after month 1, growing about $0.12 a month; PUTs ≈ 70k ≈ $0.35 |
| Athena | ~100 demo queries × ≤200 MB = 20 GB | $0.10 at $5/TB |
| CloudWatch Logs | 14-day retention, small | < $0.50 |
| **Total** | | **≈ US$1–4 per month** (verified unit prices; volumes estimated) |

Lambda and Athena prices are from the AWS pricing pages, read 2026-11-16. The S3 storage
($0.023/GB) and PUT ($0.005 per 1,000) rates are standard published rates and weren't
re-read today. Row volume is an estimate, so check it after the first live day.

## Revenue and payback
- Ask: 35,000 ALGO (≈ US$3,500, small category; the range is 5,000–50,000) for the delivered v0.1–v0.3.
  That's about 90 h at ≈ US$38/h, the same rate as the C# SDK retro proposal (120k ALGO for 12k USD).
- Retroactive funding is "immediately disbursed" after Committee approval with no Council veto.
- Timeline: build v0.2–v0.3 (≈4 weeks) → 2-week discussion → 2-week vote → payout. That's about
  8–9 weeks, inside the 3-month window if the proposal goes in by about 2026-12-21.
- Payback: one approved grant covers the ≈ US$20 sunk fees plus about 1,000+ months of this AWS bill.
  The 3% commitment comes back after the vote.
- Repeatable: maintenance and new features can be proposed again later (proactive proposals
  aren't open yet). The same ETL can be sold to Algorand teams as a hosted dataset or custom
  pipeline work.

## Risks
1. Votes: quorum is 40–70% of about 100 active xGovs. New tools with low adoption are weak candidates,
   which is why v0.3 adds a public dataset to show usage.
2. Treasury: in April 2026 the council said all plausible proposals would push the treasury negative.
   A node-staking funding model was agreed in August 2026.
3. Rules may change: a "battle-testing" period before code is grant-eligible was discussed (Aug 2026, not adopted).
4. ALGO price: paid in ALGO, so convert promptly.

## Mexico
The grant counts as income in MXN at receipt, and a later sale of the ALGO is an *enajenación*. A
contador should confirm the regime (RESICO or actividad empresarial) and the CFDI. Banxico's crypto
rules apply to financial institutions, not individuals. xGov requires KYC before proposing.
