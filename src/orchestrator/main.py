"""CLI entry point: run a task through the orchestration graph."""

from __future__ import annotations

import argparse
import json
import sys

from pydantic import ValidationError

from .config import (
    format_config_error,
    format_diagnostics_summary,
    get_settings,
    get_startup_diagnostics,
    validate_configuration,
)
from .graph import resume_task, run_task
from .observability.tracing import setup_tracing


def cli(argv: list[str] | None = None) -> int:
    actual_argv = sys.argv[1:] if argv is None else list(argv)
    if actual_argv and actual_argv[0] == "backup":
        from .persistence.backup import backup_cli

        return backup_cli(actual_argv[1:])

    parser = argparse.ArgumentParser(
        description="Run a task through the multi-agent orchestrator, or manage database with 'backup'."
    )
    parser.add_argument("task", nargs="?", help="The task to execute.")
    parser.add_argument("--trace", action="store_true", help="Print the full execution trace.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    parser.add_argument("--resume", metavar="RUN_ID", help="Resume an existing workflow execution by run ID.")
    parser.add_argument("--status", metavar="RUN_ID", help="Query status of a run ID from checkpoints.")
    parser.add_argument("--concurrency", type=int, default=None, help="Maximum specialist concurrency.")
    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Workflow execution deadline in seconds (e.g. --timeout 30.0).",
    )
    parser.add_argument("--eval", action="store_true", help="Run the offline evaluation framework.")
    parser.add_argument("--eval-cases", metavar="CASE_IDS", help="Comma-separated case IDs to evaluate.")
    parser.add_argument("--eval-tag", metavar="TAG", help="Filter evaluation cases by tag.")
    parser.add_argument("--eval-output", metavar="PATH", help="Write evaluation report JSON to file.")
    parser.add_argument(
        "--eval-min-score",
        type=float,
        default=1.0,
        help="Minimum pass rate threshold for evaluation (default: 1.0).",
    )
    parser.add_argument("--serve", action="store_true", help="Start the HTTP REST API server.")
    parser.add_argument("--host", default=None, help="Host to bind the API server to (default: 127.0.0.1).")
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Port to bind the API server to (default: 8000).",
    )
    parser.add_argument(
        "--api-token",
        default=None,
        help="Bearer token for API authentication (or set ORCHESTRATOR_API_TOKEN).",
    )
    parser.add_argument(
        "--diagnostics",
        action="store_true",
        help="Print startup configuration summary and non-sensitive diagnostics.",
    )
    args = parser.parse_args(argv)

    if args.concurrency is not None:
        if args.concurrency < 1 or args.concurrency > 128:
            print(
                f"Configuration error: --concurrency must be 1..128, got {args.concurrency}.",
                file=sys.stderr,
            )
            return 1

    if args.timeout is not None:
        if args.timeout <= 0:
            print(
                f"Configuration error: --timeout must be > 0, got {args.timeout}.",
                file=sys.stderr,
            )
            return 1

    try:
        settings = get_settings()
        validate_configuration(settings)
    except (ValidationError, ValueError) as exc:
        print(f"Configuration error: {format_config_error(exc)}", file=sys.stderr)
        return 1

    # Diagnostics inspection
    if args.diagnostics:
        if args.json:
            print(json.dumps(get_startup_diagnostics(settings), indent=2))
        else:
            print(format_diagnostics_summary(settings), file=sys.stderr)
        return 0

    setup_tracing()

    # Case 0a: Start API server
    if args.serve:
        import os

        try:
            from .api.server import run_server
        except ImportError as exc:
            print(
                f"API server dependencies not installed: {exc}. "
                "Install with: pip install 'multi-agent-orchestrator[api]'",
                file=sys.stderr,
            )
            return 1

        if args.api_token:
            os.environ["ORCHESTRATOR_API_TOKEN"] = args.api_token

        try:
            run_server(host=args.host, port=args.port)
            return 0
        except Exception as exc:
            print(f"Error starting API server: {exc}", file=sys.stderr)
            return 1

    # Case 0: Offline evaluation
    if args.eval:
        from .evaluation.runner import eval_cli

        forward_argv = []
        if args.eval_cases:
            forward_argv.extend(["--cases", args.eval_cases])
        if args.eval_tag:
            forward_argv.extend(["--tag", args.eval_tag])
        if args.eval_output:
            forward_argv.extend(["--output", args.eval_output])
        if args.eval_min_score != 1.0:
            forward_argv.extend(["--min-score", str(args.eval_min_score)])
        if args.json:
            forward_argv.append("--json")
        return eval_cli(forward_argv)

    # Case 1: Status query
    if args.status:
        from .persistence.store import CheckpointStore

        store = CheckpointStore()
        summary = store.get_run_summary(args.status)
        if not summary:
            try:
                rec = store.load_latest_checkpoint(args.status)
                summary = {
                    "run_id": rec.run_id,
                    "task": rec.state.get("task", ""),
                    "status": rec.state.get("status", "pending"),
                    "latest_step": rec.step,
                    "schema_version": rec.schema_version,
                    "created_at": rec.created_at,
                    "updated_at": rec.created_at,
                }
            except Exception:
                print(f"Error: No run found for run_id '{args.status}'", file=sys.stderr)
                return 1

        if args.json:
            print(json.dumps(summary, indent=2))
            return 0

        print(f"\n=== STATUS: {summary['run_id']} ===")
        print(
            f"status={summary['status']}  latest_step={summary['latest_step']}  "
            f"updated_at={summary['updated_at']}"
        )
        print(f"task={summary['task']!r}")
        return 0

    # Case 2: Resume existing run
    if args.resume:
        print(f"› resuming run={args.resume!r} provider={settings.active_provider}", file=sys.stderr)
        try:
            kwargs = {}
            if args.concurrency is not None:
                kwargs["max_concurrency"] = args.concurrency
            if args.timeout is not None:
                kwargs["timeout"] = args.timeout
            result = resume_task(args.resume, **kwargs)
        except KeyboardInterrupt:
            print("Workflow cancelled by user (SIGINT).", file=sys.stderr)
            return 130
        except Exception as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1
    # Case 3: Run new task
    else:
        if not args.task:
            parser.print_help(file=sys.stderr)
            return 1

        print(f"› provider={settings.active_provider}  task={args.task!r}", file=sys.stderr)
        try:
            kwargs = {}
            if args.concurrency is not None:
                kwargs["max_concurrency"] = args.concurrency
            if args.timeout is not None:
                kwargs["timeout"] = args.timeout
            result = run_task(args.task, **kwargs)
        except KeyboardInterrupt:
            print("Workflow cancelled by user (SIGINT).", file=sys.stderr)
            return 130
        except Exception as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1

    if args.json:
        print(json.dumps(result, indent=2, default=str))
        return 0

    print("\n=== ANSWER ===")
    print(result.get("answer", "(none)"))
    print("\n=== REVIEW ===")
    print(
        f"status={result.get('status')}  quality={result.get('quality')}  "
        f"confidence={result.get('confidence')}  escalated={result.get('escalated')}  "
        f"reason={result.get('escalation_reason', '')}"
    )

    if "usage" in result:
        u = result["usage"]
        cost_val = u.get("estimated_cost_usd")
        cost_str = f"${cost_val:.6f}" if cost_val is not None else "unknown"
        if u.get("has_unknown_cost") and cost_val is not None:
            cost_str += " (partial/unknown)"
        print("\n=== USAGE ===")
        print(
            f"tokens={u.get('total_tokens', 0)} "
            f"(in={u.get('total_input_tokens', 0)}, out={u.get('total_output_tokens', 0)})  "
            f"calls={u.get('successful_requests', 0)}  est_cost={cost_str}"
        )

    if args.trace:
        print("\n=== TRACE ===")
        for evt in result.get("trace", []):
            print(json.dumps(evt))
    return 0


if __name__ == "__main__":
    raise SystemExit(cli())
