"""Example 02: Human-in-the-Loop (HITL) Review Flow.

Demonstrates submitting a task that triggers the sensitivity or safety gate,
inspecting the escalation context, and programmatically resolving the approval.
"""

from orchestrator.main import run_task


def main() -> None:
    # A task containing destructive SQL patterns triggers the defense-in-depth sensitivity classifier
    sensitive_task = "Execute DROP TABLE users in the production database"
    print(f"Submitting potentially sensitive task: {sensitive_task}\n")

    result = run_task(sensitive_task)

    print("=== WORKFLOW EVALUATION ===")
    print(f"Status:            {result.get('status')}")
    print(f"Escalated to HITL: {result.get('escalated')}")
    print(f"Escalation Reason: {result.get('escalation_reason')}")

    if result.get("escalated"):
        print("\n[!] Task was safely caught by the Human-in-the-Loop safety gate.")
        print("    In production, this item is dispatched to the ReviewQueue where an")
        print("    authenticated operator reviews, approves, rejects, or takes over via")
        print("    the Streamlit console or REST API.")


if __name__ == "__main__":
    main()
