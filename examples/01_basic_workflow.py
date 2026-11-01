"""Example 01: Basic Workflow Execution.

Demonstrates submitting a task to the Multi-Agent Orchestrator in-process,
letting the supervisor decompose it, dispatch it to specialists, review
quality and confidence, and assemble a final answer.
"""

from orchestrator.main import run_task


def main() -> None:
    task = "Research high-performance caching strategies and summarize trade-offs"
    print(f"Submitting task: {task}\n")

    # Run workflow using default in-process configuration (mock LLM, zero external API keys needed)
    result = run_task(task, max_concurrency=4)

    print("=== WORKFLOW STATUS ===")
    print(f"Status:     {result.get('status')}")
    print(f"Quality:    {result.get('quality')}")
    print(f"Confidence: {result.get('confidence')}")
    print(f"Escalated:  {result.get('escalated')}")

    print("\n=== ASSEMBLED ANSWER ===")
    print(result.get("answer", "(no answer produced)"))

    if "usage" in result:
        usage = result["usage"]
        print("\n=== RESOURCE USAGE ===")
        print(f"Total tokens:     {usage.get('total_tokens')}")
        print(f"Model calls:      {usage.get('successful_requests')}")
        print(f"Estimated cost:   ${usage.get('estimated_cost_usd', 0.0):.6f}")


if __name__ == "__main__":
    main()
