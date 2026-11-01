"""Example 03: REST API Usage.

Demonstrates programmatically interacting with the orchestrator API:
checking service health, submitting a workflow run, and inspecting checkpoints.
"""

from fastapi.testclient import TestClient

from orchestrator.api.server import create_app


def main() -> None:
    # Instantiate the FastAPI app
    app = create_app()
    client = TestClient(app)

    print("1. Querying /health endpoint...")
    resp = client.get("/health")
    print(f"   Status Code: {resp.status_code}")
    print(f"   Response:    {resp.json()}\n")

    print("2. Submitting workflow run via POST /runs...")
    payload = {"task": "Explain the difference between synchronous and asynchronous execution"}
    resp = client.post("/runs", json=payload)
    print(f"   Status Code: {resp.status_code}")
    run_data = resp.json()
    run_id = run_data.get("run_id")
    print(f"   Run ID:      {run_id}")
    print(f"   Run Status:  {run_data.get('status')}\n")

    print(f"3. Querying checkpoints for run '{run_id}' via GET /runs/{run_id}/checkpoints...")
    resp = client.get(f"/runs/{run_id}/checkpoints")
    print(f"   Status Code: {resp.status_code}")
    print(f"   Checkpoints: {resp.json().get('total_steps')} step snapshots recorded.")


if __name__ == "__main__":
    main()
