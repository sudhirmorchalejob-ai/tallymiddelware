# Tally-Connect — TallyScrapper Middleware

A desktop middleware agent that syncs data from **Tally ERP 9 / Tally Prime** (via its XML HTTP interface) into a **PostgreSQL** database, with true incremental sync, per-company summaries, and an interactive CLI.

## Features

- **License-protected login** (authenticates against the Tally-Connect license system)
- **Developer Mode** — shows full technical logs; normal users see clean, friendly output only
- **True incremental sync** — window-based checkpoints, resume from where it stopped, and a recent re-fetch window (`RECENT_WINDOW_DAYS`, default 30) so nothing is missed
- **Synced entities** — Ledgers, Bills Receivable, Bills Payable, Vouchers, Invoices, Orders, Inventory Items
- **Accurate "New records" counting** in every sync summary (Fetched / New / Failed / Skipped / In DB)
- **Per-company separate summaries** with voucher-type breakdown
- **Auto-sync loop** — keeps syncing on an interval until you stop the terminal
- **`[B] Back` navigation** on every pre-sync configuration screen — fix mistakes without restarting

## Getting Started

```bash
npm install
npm start
```

On first launch the agent will:

1. Show the **Authentication menu**
2. Ask for **database settings** (Local or External PostgreSQL) and test the connection
3. Show the **main menu**:

```
[1] Start Sync
[2] Database Settings (Change Database Configuration)
[3] Tally Port Settings (Change Tally HTTP Port)
[4] Reset & Full Re-sync (clears all checkpoints)
[5] Exit
```

### Sync flow

1. Pick companies (e.g. `1,3` for two of four companies, `0` for ALL, `B` to go back)
2. Pick data types to sync (`0` = ALL)
3. Set auto-sync interval in minutes (`0` = continuous, default every 5 min)

Type `B` at any prompt to step back to the previous screen.

## Developer Login Credentials

The Authentication menu has a **Developer Login** option (option `2`) that enables
Developer Mode (full technical log visibility). It does **not** bypass the normal
license login.

| Field    | Value              |
| -------- | ------------------ |
| Email    | `sudhir@gmail.com` |
| Password | `sudhirRA@2026`    |

> The credentials can be overridden with the `DEVELOPER_EMAIL` and
> `DEVELOPER_PASSWORD` environment variables without changing the code.

## Building the Windows EXE

Requires [Node.js](https://nodejs.org) and `pkg`:

```bash
npx pkg .
```

The executable is written to `dist/` (configured in `package.json`,
target `node18-win-x64`).

## Project Structure

| File                | Purpose                                        |
| ------------------- | ---------------------------------------------- |
| `agent.cjs`         | Main middleware (CLI, sync engine, DB writer)  |
| `test-helpers.cjs`  | Unit tests for checkpoints / windows / dates   |
| `package.json`      | Dependencies + pkg build config                |

Logs are written to the OS temp directory (`tally-agent-logs`) and are always
complete — Developer Mode only controls what is printed to the console.
