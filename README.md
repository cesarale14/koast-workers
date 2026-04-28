# koast-workers

Background workers for [Koast](https://app.koasthq.com) (formerly StayCommand). Companion repo to [`cesarale14/koast`](https://github.com/cesarale14/koast) — the Next.js app + database migrations live there; long-running and scheduled work lives here.

Deployed to `/home/ubuntu/koast-workers/` on the Virginia VPS (`44.195.218.19`). Workers run as `systemd` units; timers schedule them.

## Workers

| Worker | Cadence | Entry point | Log path |
|---|---|---|---|
| `pricing_validator.py` | daily 06:00 UTC (10:00 ET) | systemd timer `koast-pricing-validator.timer` | `/var/log/koast/pricing-validator.log` |
| `pricing_performance_reconciler.py` | nightly 02:30 UTC | systemd timer `koast-pricing-performance-reconciler.timer` | `/var/log/koast/pricing-performance.log` |
| `booking_sync.py` | every 15 min | systemd timer (legacy `koast-bookings.timer` per `status.sh`) | `/var/log/koast/booking-sync.log` |
| `reviews_sync.py` | every 20 min (timer **not yet enabled** — supervised first-run gate) | `koast-reviews-sync.timer` | `/var/log/koast/reviews.log` |
| `messages_sync.py` | every 60 min (timer **not yet enabled**) | `koast-messages-sync.timer` | `/var/log/koast/messages.log` |
| `messaging_executor.py` | hourly (timer **not yet enabled** — supervised first-run gate) | `koast-messaging-executor.timer` | `/var/log/koast/messaging-executor.log` |
| `market_sync.py` | **disabled** (`KOAST_DISABLE_AIRROI=true`) | n/a | n/a |
| `pricing_worker.py` | manual / ad-hoc | n/a | stdout |
| `ical_parser.py` | library only (used by booking_sync) | n/a | n/a |
| `db.py` | shared psycopg2 helpers | n/a | n/a |
| `status.sh` | health-check script | run manually: `./status.sh` | n/a |

The reviews / messages / messaging-executor timers ship with their unit files in this repo but are deliberately **not enabled** on the VPS. Manual run + log inspection is the supervised-first-run gate before flipping a timer on. See the koast repo's `references/tech-debt.md` "Worker timers not yet enabled" entry.

## systemd units

`./systemd/` holds the unit + timer files for the workers controlled by this repo. Per-worker pattern:

```
systemd/
  koast-<worker>.service   # ExecStart points at the .py file in this repo
  koast-<worker>.timer     # OnCalendar cadence
```

Deployment:

```bash
# Symlink (NOT copy) so future `git pull` updates the active units.
sudo ln -s /home/ubuntu/koast-workers/systemd/koast-<worker>.service /etc/systemd/system/
sudo ln -s /home/ubuntu/koast-workers/systemd/koast-<worker>.timer   /etc/systemd/system/
sudo systemctl daemon-reload
# Supervised first run BEFORE enabling the timer:
sudo systemctl start koast-<worker>.service
sudo journalctl -u koast-<worker>.service --no-pager -n 50
# If the run looks clean:
sudo systemctl enable --now koast-<worker>.timer
```

Some legacy units (`koast-pricing.*`, `koast-market.*`, `koast-bookings.*`) live directly in `/etc/systemd/system/` from before this repo existed. They are NOT in this repo's `systemd/` dir; check `systemctl list-timers --all | grep -i koast` for the full picture before touching units. `market_sync` is intentionally disabled (AirROI paused).

## Secrets

All secrets live in `~/koast-workers/.env` on the VPS. Never committed. See `.env.example` for the variable list.

The workers all load via `python-dotenv` at the top of each entry-point file; secrets are accessed via `os.environ[...]` and never hardcoded.

## Local setup (for development on a fresh machine)

```bash
git clone git@github.com:cesarale14/koast-workers.git
cd koast-workers
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Fill .env with real values (ask Cesar; secrets are not in this repo).
python3 pricing_validator.py   # or whichever worker
```

## Cross-repo coordination with `cesarale14/koast`

When a session changes both repos (e.g. messaging executor + its corresponding API route landed in Session 8a as commit `c870cfa` in koast and the initial worker code in this repo), commit each to its own repo and reference the companion commit hash in the body. The "Two-headed sync subsystem" playbook in the koast repo's `references/playbooks.md` captures this pattern.

## License

Private. Property of cesarale14 / Koast.
