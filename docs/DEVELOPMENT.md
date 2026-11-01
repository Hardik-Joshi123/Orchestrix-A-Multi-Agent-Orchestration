# Developer Guide & Local Workflow

This guide covers setting up your local environment, running the multi-agent orchestrator, executing
tests, and adhering to coding standards.

---

## 1. Prerequisites

- **Python**: `>=3.10` (tested on `3.10`, `3.11`, `3.12`)
- **Virtual Environment Tool**: Standard `venv` module
- **Git**: Version 2.30+
- **External Dependencies**: **None required.** The orchestrator runs 100% offline with zero external API
  keys or external backing services.

---

## 2. Environment Setup

### Clone and Create Virtual Environment

#### Windows (PowerShell)
```powershell
git clone https://github.com/Hardik-Joshi123/multi-agent-orchestrator.git
cd multi-agent-orchestrator
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

#### macOS / Linux (Bash)
```bash
git clone https://github.com/Hardik-Joshi123/multi-agent-orchestrator.git
cd multi-agent-orchestrator
python3 -m venv .venv
source .venv/bin/activate
```

### Install Dependencies

Choose your desired dependency tier:

```bash
# Minimal core install (Pydantic & Settings only; zero heavy dependencies)
pip install -e .

# Recommended development install (Core + FastAPI + Pytest + Ruff)
pip install -e ".[api,dev]"

# Streamlit UI review console
pip install -e ".[ui]"

# All optional integrations (LLM SDKs, LangGraph, Redis, Chroma, Celery, OTel)
pip install -e ".[all]"

# Pinned production dependencies
pip install -r requirements.txt
```

### Configure Local Environment

Copy the annotated configuration template:
```bash
cp .env.example .env
```
By default, `DEFAULT_LLM_PROVIDER=mock`, enabling zero-cost, zero-latency local execution.

---

## 3. Running the Project Locally

### 1. Execute CLI Workflows

```bash
# Run a task with human-readable CLI summary
python -m orchestrator "Explain the CAP theorem and distributed consensus trade-offs"

# Emit machine-readable JSON output on stdout (logs on stderr)
python -m orchestrator "Analyze database isolation levels" --json

# Run diagnostics check without executing workflow
python -m orchestrator --diagnostics
```

### 2. Run the FastAPI REST Server

```bash
# Start local API server on http://127.0.0.1:8000
python -m orchestrator --serve

# Start with a required Bearer token:
python -m orchestrator --serve --api-token "dev-secret-token"
```

Open interactive Swagger UI at [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs).

### 3. Launch the Streamlit Human Review Console

```bash
streamlit run ui/review_app.py
```

Open the console at [http://localhost:8501](http://localhost:8501). Reviewer credentials default to
`dev_admin:dev_token_12345`.

---

## 4. Code Quality & Linting Standards

The repository enforces strict linting and code style standards:
- **Linter & Formatter**: [Ruff](https://github.com/astral-sh/ruff)
- **Line Length**: Strictly $\le 110$ characters (`line-length = 110`)
- **Python Version Target**: Python 3.10 (`py310`)

### Running Code Quality Checks

```bash
# Run Ruff lint checks
python -m ruff check src/ tests/ examples/

# Automatically fix fixable lint issues
python -m ruff check --fix src/ tests/ examples/

# Check formatting without altering files
python -m ruff format --check src/ tests/ examples/
```

---

## 5. Automated Testing

### Test Suite Organization

The `tests/` directory contains unit, integration, and failure-injection tests:
- `tests/test_end_to_end.py`: End-to-end multi-agent orchestration lifecycle
- `tests/test_api.py`: FastAPI routes, health probes, and lifecycle management
- `tests/test_api_security.py`: Bearer authentication and network binding safety
- `tests/test_hitl.py`: Human-in-the-loop review queues, timeouts, and escalation
- `tests/test_signed_backup_manifest.py`: Cryptographic HMAC-SHA256 manifests and key rotation
- `tests/test_database_backup_restore.py`: SQLite online backup, WAL, vacuum, and restore
- `tests/test_restore_drill.py`: Automated isolated restore drill verification
- `tests/test_celery_execution.py`: Celery queue submission and background workers

### Running Tests

```bash
# Run the entire test suite
python -m pytest tests/

# Run a specific test module with verbose output
python -m pytest tests/test_signed_backup_manifest.py -v

# Run with test coverage
python -m pytest --cov=orchestrator tests/
```

---

## 6. Offline Evaluation Framework

Evaluate the orchestrator across deterministic benchmark test cases:
```bash
# Run full evaluation benchmark
python -m orchestrator --eval

# Emit evaluation benchmark results as JSON
python -m orchestrator --eval --json

# Run evaluation filtered by category tag (e.g. hitl, resume, cycle)
python -m orchestrator --eval --eval-tag hitl
```
