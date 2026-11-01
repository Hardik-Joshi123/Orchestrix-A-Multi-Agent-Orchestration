"""Interactive Multi-Agent Orchestration Dashboard and Human-in-the-Loop Review Console.

Integrates the Task 18 HTTP API for workflow creation, polling, resumption,
checkpoint inspection, and evaluation telemetry, while preserving the existing
local HITL review queue and tamper-evident audit trail.

Run:
    streamlit run ui/review_app.py
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

# Allow `streamlit run ui/review_app.py` without an editable install.
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_PROJECT_ROOT / "src"))
sys.path.insert(0, str(_PROJECT_ROOT))

import streamlit as st  # noqa: E402

from orchestrator.api.client import (  # noqa: E402
    ApiAuthenticationError,
    ApiConflictError,
    ApiConnectionError,
    ApiNotFoundError,
    ApiPayloadTooLargeError,
    ApiRateLimitError,
    ApiValidationError,
    OrchestratorApiClient,
)
from orchestrator.hitl.audit import AuditChain  # noqa: E402
from orchestrator.hitl.auth import (  # noqa: E402
    AuthError,
    authenticate,
    session_expired,
)
from orchestrator.hitl.queue import ReviewQueue  # noqa: E402
from ui.dashboard_helpers import (  # noqa: E402
    check_resume_eligibility,
    classify_subtasks,
    format_checkpoint_records,
    format_cost_display,
    format_status_badge,
    sanitize_ui_error,
)

# -----------------------------------------------------------------------------
# Page Configuration & Styling
# -----------------------------------------------------------------------------

st.set_page_config(
    page_title="Multi-Agent Orchestrator Dashboard",
    page_icon="🤖",
    layout="wide",
)

st.title("🤖 Multi-Agent Orchestration Dashboard")
st.caption("Workflow Execution · Checkpoint Resumption · Evaluation Benchmarks · HITL Safety Console")

_AUTH_REQUIRED_MSG = (
    "🔒 **Authentication Required**: API requires a valid Bearer token. "
    "Please configure the token in the sidebar."
)


# -----------------------------------------------------------------------------
# Session State Initialization
# -----------------------------------------------------------------------------

if "active_run_id" not in st.session_state:
    st.session_state["active_run_id"] = ""
if "run_cache" not in st.session_state:
    st.session_state["run_cache"] = {}
if "submitting_run" not in st.session_state:
    st.session_state["submitting_run"] = False
if "resuming_run" not in st.session_state:
    st.session_state["resuming_run"] = False
if "last_error" not in st.session_state:
    st.session_state["last_error"] = None


# -----------------------------------------------------------------------------
# Reviewer Authentication Gate (for HITL Actions)
# -----------------------------------------------------------------------------


def _get_authenticated_reviewer() -> str | None:
    """Check reviewer session or return None if unauthenticated."""
    reviewer = st.session_state.get("reviewer")
    if reviewer:
        if session_expired(st.session_state.get("reviewer_at", 0.0)):
            st.session_state.pop("reviewer", None)
            st.session_state.pop("reviewer_at", None)
            st.warning("Reviewer session expired. Please sign in again.")
            return None
        return reviewer
    return None


# -----------------------------------------------------------------------------
# Sidebar Configuration & Health Telemetry
# -----------------------------------------------------------------------------

with st.sidebar:
    st.header("⚙️ API Configuration")
    api_base_url = st.text_input(
        "API Base URL",
        value="http://127.0.0.1:8000",
        help="Base URL of the running Orchestrator HTTP API (Task 18).",
    ).strip().rstrip("/")

    api_token_val = st.text_input(
        "API Bearer Token",
        value=os.environ.get("ORCHESTRATOR_API_TOKEN", ""),
        type="password",
        help="Optional Bearer token when API authentication is enabled.",
    ).strip()

    client = OrchestratorApiClient(
        base_url=api_base_url,
        api_token=api_token_val,
        timeout=15.0,
    )

    # Health Check Telemetry
    try:
        health = client.get_health()
        auth_status = "Bearer Enforced" if getattr(health, "auth_required", False) else "Loopback/Open"
        st.success(
            f"🟢 **API Online** (v{health.version})\n\n"
            f"- Provider: `{health.active_llm_provider}`\n"
            f"- Checkpoints: `{health.checkpoint_backend}`\n"
            f"- Active Runs: `{health.active_runs_count}`\n"
            f"- Auth: `{auth_status}`"
        )
    except ApiConnectionError:
        st.error(
            f"🔴 **API Offline**\n\nCannot connect to `{api_base_url}`.\n\n"
            "Start the API server:\n```bash\norchestrator --serve\n```"
        )
    except Exception as exc:
        st.warning(f"⚠️ Health check warning: {sanitize_ui_error(exc)}")

    st.divider()

    # Specialist Concurrency Limits
    st.subheader("Execution Controls")
    default_concurrency = st.slider(
        "Default Max Concurrency",
        min_value=1,
        max_value=8,
        value=4,
        help="Maximum concurrent specialist tasks permitted per workflow.",
    )

    st.divider()

    # Reviewer Login / Status
    st.subheader("Reviewer Console")
    current_reviewer = _get_authenticated_reviewer()
    if current_reviewer:
        st.success(f"Reviewer: **{current_reviewer}**")
        if st.button("Sign out"):
            st.session_state.pop("reviewer", None)
            st.session_state.pop("reviewer_at", None)
            st.rerun()
    else:
        with st.expander("Reviewer Sign-in", expanded=False):
            with st.form("reviewer_login_form"):
                name_input = st.text_input("Reviewer Name")
                token_input = st.text_input("Token", type="password")
                submit_login = st.form_submit_button("Sign In")
                if submit_login:
                    try:
                        rev = authenticate(name_input, token_input)
                        st.session_state["reviewer"] = rev.name
                        st.session_state["reviewer_at"] = time.time()
                        st.rerun()
                    except AuthError as exc:
                        st.error(f"Sign-in failed: {exc}")


# -----------------------------------------------------------------------------
# Main Navigation Tabs
# -----------------------------------------------------------------------------

tab_run, tab_resume, tab_eval, tab_hitl = st.tabs(
    [
        "🚀 Workflow Execution",
        "🔄 Resumption & Checkpoints",
        "📊 Evaluation & Telemetry",
        "🧠 HITL Review Queue",
    ]
)


# =============================================================================
# TAB 1: Workflow Execution & Tracking
# =============================================================================

with tab_run:
    st.subheader("Submit New Workflow")

    with st.form("create_run_form"):
        task_instruction = st.text_area(
            "Task Instruction",
            placeholder="e.g. Analyze memory consumption, optimize SQL queries, and generate report.",
            height=100,
        )
        col_c1, col_c2 = st.columns([1, 1])
        with col_c1:
            run_concurrency = st.number_input(
                "Specialist Concurrency Limit",
                min_value=1,
                max_value=8,
                value=default_concurrency,
            )
        with col_c2:
            run_background = st.checkbox(
                "Run in background (asynchronous)",
                value=False,
                help="If checked, returns immediately with HTTP 202 Accepted and executes in background.",
            )

        submit_disabled = st.session_state["submitting_run"]
        submitted = st.form_submit_button("Submit Workflow", type="primary", disabled=submit_disabled)

    if submitted:
        clean_task = task_instruction.strip()
        if not clean_task:
            st.warning("Please enter a non-empty task instruction.")
        else:
            st.session_state["submitting_run"] = True
            try:
                with st.spinner("Submitting workflow to orchestrator..."):
                    run_resp = client.create_run(
                        task=clean_task,
                        max_concurrency=int(run_concurrency),
                        background=run_background,
                    )
                st.session_state["active_run_id"] = run_resp.run_id
                st.session_state["run_cache"][run_resp.run_id] = run_resp.model_dump()
                st.success(f"Workflow submitted! Run ID: `{run_resp.run_id}`")
                st.rerun()
            except ApiAuthenticationError:
                st.error(_AUTH_REQUIRED_MSG)
            except ApiRateLimitError as exc:
                retry_msg = f" Please retry in {exc.retry_after} seconds." if exc.retry_after else ""
                st.error(f"⏳ Rate limit exceeded: {sanitize_ui_error(exc)}.{retry_msg}")
            except ApiPayloadTooLargeError as exc:
                st.error(f"📦 Request payload too large: {sanitize_ui_error(exc)}")
            except ApiConflictError as exc:
                st.error(f"Execution Conflict: {sanitize_ui_error(exc)}")
            except ApiValidationError as exc:
                st.error(f"Validation Error: {sanitize_ui_error(exc)}")
            except ApiConnectionError as exc:
                st.error(f"Connection Error: {sanitize_ui_error(exc)}")
            except Exception as exc:
                st.error(f"Failed to submit workflow: {sanitize_ui_error(exc)}")
            finally:
                st.session_state["submitting_run"] = False

    st.divider()

    # Active Workflow Inspection & Polling
    st.subheader("Workflow Status & Tracking")

    col_track1, col_track2 = st.columns([3, 1])
    with col_track1:
        tracked_id = st.text_input(
            "Run ID to Track",
            value=st.session_state["active_run_id"],
            placeholder="e.g. run-8d889b27b50a",
        ).strip()
    with col_track2:
        st.write("")
        st.write("")
        refresh_btn = st.button("🔄 Refresh Status", key="btn_refresh_status")

    auto_refresh = st.checkbox(
        "Auto-refresh running workflow (3s poll)",
        value=False,
        help="Polls the API automatically while the current workflow is executing.",
    )

    current_data = None
    if tracked_id:
        st.session_state["active_run_id"] = tracked_id
        if refresh_btn or tracked_id not in st.session_state["run_cache"] or auto_refresh:
            try:
                resp = client.get_run(tracked_id)
                current_data = resp.model_dump()
                st.session_state["run_cache"][tracked_id] = current_data
            except ApiAuthenticationError:
                st.error(_AUTH_REQUIRED_MSG)
            except ApiNotFoundError:
                st.error(f"Run ID `{tracked_id}` was not found in the checkpoint database.")
            except ApiConnectionError as exc:
                st.error(f"Connection Error: {sanitize_ui_error(exc)}")
            except Exception as exc:
                st.error(f"Error fetching run: {sanitize_ui_error(exc)}")
        else:
            current_data = st.session_state["run_cache"].get(tracked_id)

    if current_data:
        status_info = format_status_badge(current_data.get("status"))

        # Status Summary Header
        st.markdown(
            f"### Status: {status_info['icon']} **{status_info['label']}** "
            f"· Step: `{current_data.get('latest_step', 0)}`"
        )
        st.caption(
            f"Task: *{current_data.get('task')}* | "
            f"Created: `{current_data.get('created_at', 'N/A')}` | "
            f"Updated: `{current_data.get('updated_at', 'N/A')}`"
        )

        # HITL Escalation Read-Only Telemetry
        hitl_meta = current_data.get("hitl")
        if hitl_meta and hitl_meta.get("escalated"):
            st.warning(
                f"🔔 **Escalation Telemetry**: Workflow escalated for reason: "
                f"`{hitl_meta.get('escalation_reason', 'unspecified')}` "
                f"(Subtask: `{hitl_meta.get('escalated_subtask_id', 'all')}`, "
                f"Required Level: `{hitl_meta.get('approval_level', 'approve_action')}`).\n\n"
                f"*Action Required: Switch to the **HITL Review Queue** tab to inspect and resolve.*"
            )

        # Subtasks Breakdown
        subtasks = classify_subtasks(current_data)
        col_s1, col_s2, col_s3 = st.columns(3)
        with col_s1:
            st.markdown(f"**Completed Subtasks ({len(subtasks['completed'])})**")
            if subtasks["completed"]:
                for s in subtasks["completed"]:
                    st.success(f"✅ `{s}`")
            else:
                st.info("None completed")
        with col_s2:
            st.markdown(f"**Failed Subtasks ({len(subtasks['failed'])})**")
            if subtasks["failed"]:
                for s in subtasks["failed"]:
                    st.error(f"❌ `{s}`")
            else:
                st.info("None failed")
        with col_s3:
            st.markdown(f"**Blocked Subtasks ({len(subtasks['blocked'])})**")
            if subtasks["blocked"]:
                for s in subtasks["blocked"]:
                    st.warning(f"🚫 `{s}`")
            else:
                st.info("None blocked")

        # Synthesized Answer / Diagnostics
        st.markdown("#### Final Output & Diagnostics")
        ans = current_data.get("answer")
        if ans:
            st.markdown(ans)
        else:
            if current_data.get("status") == "running":
                st.info("Workflow is actively executing. Output will be displayed upon completion.")
            else:
                st.info("No synthesized answer output available for this run state.")

        # Review Scores & Usage
        review_data = current_data.get("review") or {}
        usage_data = format_cost_display(current_data.get("usage"))

        col_m1, col_m2, col_m3, col_m4 = st.columns(4)
        with col_m1:
            q_val = review_data.get("quality")
            st.metric("Quality Score", f"{q_val:.2f}" if q_val is not None else "N/A")
        with col_m2:
            c_val = review_data.get("confidence")
            st.metric("Confidence Score", f"{c_val:.2f}" if c_val is not None else "N/A")
        with col_m3:
            st.metric("Total Tokens", f"{usage_data['total_tokens']:,}")
        with col_m4:
            st.metric("Estimated Cost", usage_data["cost_display"])

        st.caption(f"Usage telemetry: {usage_data['notice']}")

        # Auto-refresh loop guard
        if auto_refresh and current_data.get("status") == "running":
            time.sleep(3)
            st.rerun()


# =============================================================================
# TAB 2: Resumption & Checkpoint Inspection
# =============================================================================

with tab_resume:
    st.subheader("Checkpoint Resumption & Step History")

    resume_target_id = st.text_input(
        "Run ID to Inspect or Resume",
        value=st.session_state["active_run_id"],
        key="input_resume_target",
        placeholder="e.g. run-8d889b27b50a",
    ).strip()

    if resume_target_id:
        st.session_state["active_run_id"] = resume_target_id

        # Resumption Eligibility Assessment
        cached = st.session_state["run_cache"].get(resume_target_id)
        if not cached:
            try:
                cached = client.get_run(resume_target_id).model_dump()
                st.session_state["run_cache"][resume_target_id] = cached
            except Exception:
                cached = {}

        is_eligible, reason_msg = check_resume_eligibility(cached)

        col_res1, col_res2 = st.columns([3, 1])
        with col_res1:
            if is_eligible:
                st.success(f"Eligibility: **{reason_msg}**")
            else:
                st.warning(f"Resumption Ineligible: **{reason_msg}**")

        with col_res2:
            res_concurrency = st.number_input(
                "Resume Concurrency",
                min_value=1,
                max_value=8,
                value=default_concurrency,
                key="num_res_concurrency",
            )
            resume_clicked = st.button(
                "▶️ Resume Workflow",
                type="primary",
                disabled=(not is_eligible or st.session_state["resuming_run"]),
            )

        if resume_clicked:
            st.session_state["resuming_run"] = True
            try:
                with st.spinner(f"Resuming run `{resume_target_id}` from latest checkpoint..."):
                    resumed_resp = client.resume_run(
                        run_id=resume_target_id,
                        max_concurrency=int(res_concurrency),
                    )
                st.session_state["run_cache"][resume_target_id] = resumed_resp.model_dump()
                st.success(f"Resumed successfully! Terminal status: `{resumed_resp.status}`")
                st.rerun()
            except ApiAuthenticationError:
                st.error(_AUTH_REQUIRED_MSG)
            except ApiRateLimitError as exc:
                retry_msg = f" Please retry in {exc.retry_after} seconds." if exc.retry_after else ""
                st.error(f"⏳ Rate limit exceeded: {sanitize_ui_error(exc)}.{retry_msg}")
            except ApiPayloadTooLargeError as exc:
                st.error(f"📦 Request payload too large: {sanitize_ui_error(exc)}")
            except ApiConflictError as exc:
                st.error(f"Conflict: {sanitize_ui_error(exc)}")
            except ApiNotFoundError as exc:
                st.error(f"Not Found: {sanitize_ui_error(exc)}")
            except Exception as exc:
                st.error(f"Resume Failed: {sanitize_ui_error(exc)}")
            finally:
                st.session_state["resuming_run"] = False

        st.divider()

        # Checkpoints Transition History
        st.markdown("#### Checkpoint Transition History")
        try:
            cp_resp = client.get_checkpoints(resume_target_id)
            records = format_checkpoint_records([c.model_dump() for c in cp_resp.checkpoints])
            if records:
                st.caption(f"Total Steps Recorded: `{cp_resp.total_steps}`")
                st.table(records)
                st.info(
                    "🔒 Privacy Guard: Step transitions and timestamps are displayed without "
                    "exposing raw state payloads, memory contents, or secrets."
                )
            else:
                st.info("No checkpoint records found for this run ID.")
        except ApiAuthenticationError:
            st.error(_AUTH_REQUIRED_MSG)
        except ApiNotFoundError:
            st.info("Run ID not found in checkpoint database.")
        except Exception as exc:
            st.error(f"Failed to fetch checkpoints: {sanitize_ui_error(exc)}")
    else:
        st.info("Enter or select a Run ID above to inspect checkpoints and evaluate resume eligibility.")


# =============================================================================
# TAB 3: Evaluation Dataset & Telemetry
# =============================================================================

with tab_eval:
    st.subheader("Evaluation Dataset & Offline Telemetry")

    col_tag1, col_tag2 = st.columns([2, 1])
    with col_tag1:
        selected_tag = st.selectbox(
            "Filter Evaluation Cases by Tag",
            options=[
                "(All Tags)",
                "single",
                "multi",
                "concurrency",
                "planning",
                "retry",
                "dependencies",
                "hitl",
                "partial",
                "resume",
            ],
        )
    with col_tag2:
        st.write("")
        st.write("")
        reload_eval = st.button("🔄 Reload Cases", key="btn_reload_eval")

    tag_param = None if selected_tag == "(All Tags)" else selected_tag

    try:
        eval_resp = client.get_evaluation(tag=tag_param)
        st.markdown(
            f"**Dataset Version:** `{eval_resp.dataset_version}` · "
            f"**Total Cases Matching Filter:** `{eval_resp.total_cases}`"
        )

        for case in eval_resp.cases:
            with st.expander(f"📌 {case.case_id} — {case.name}", expanded=False):
                st.markdown(f"**Description:** {case.description}")
                st.markdown(f"**Expected Terminal Status:** `{case.expected_status}`")
                st.markdown(f"**Tags:** {', '.join(f'`{t}`' for t in case.tags)}")

        st.divider()
        st.markdown("#### Latest Evaluation Benchmark Report")
        if eval_resp.latest_summary:
            st.json(eval_resp.latest_summary)
        else:
            st.info(
                "ℹ️ No cached evaluation report available from the API. "
                "To run the offline evaluation framework and generate benchmark metrics, execute:\n\n"
                "```bash\norchestrator --eval\n```"
            )
    except ApiAuthenticationError:
        st.error(_AUTH_REQUIRED_MSG)
    except ApiConnectionError as exc:
        st.error(f"Cannot load evaluation dataset: {sanitize_ui_error(exc)}")
    except Exception as exc:
        st.error(f"Error loading evaluation telemetry: {sanitize_ui_error(exc)}")


# =============================================================================
# TAB 4: HITL Review Queue (Preserved Review Surface)
# =============================================================================

with tab_hitl:
    st.subheader("Human-in-the-Loop Review Queue")
    st.caption(
        "Inspect pending escalations, verify reasoning and memories, and apply attributable decisions."
    )

    queue = ReviewQueue()
    pending_items = queue.pending()

    reviewer_identity = _get_authenticated_reviewer()
    if not reviewer_identity:
        st.warning("⚠️ Reviewer authentication required to resolve items. Please sign in via the sidebar.")

    if not pending_items:
        st.info("No items awaiting review. Submit a workflow that triggers an escalation to inspect.")

    for idx, item in enumerate(pending_items):
        item_id = getattr(item, "item_id", None) or item.task_id or f"item-{idx}"
        with st.container(border=True):
            cols = st.columns([3, 1])
            with cols[0]:
                st.markdown(f"**Task:** {item.task}")
                st.markdown(
                    f"**Reason:** `{item.reason}`  ·  **Level:** `{item.level}`  ·  "
                    f"**Subtask:** `{item.subtask_id}`"
                )
                st.markdown("**Proposed Action / Reasoning:**")
                st.code(item.proposed_action or "(empty)")
                if item.memories:
                    st.markdown("**Relevant Memories:**")
                    for m in item.memories:
                        st.markdown(f"- {m}")
            with cols[1]:
                st.metric("Confidence", f"{item.confidence:.2f}")
                st.metric("Quality", f"{item.quality:.2f}")

                decision = st.radio(
                    "Decision",
                    ["approve", "reject", "edit", "take_over"],
                    key=f"dec-{item_id}",
                )

                note = ""
                if decision in ("edit", "take_over"):
                    label = "Revised action" if decision == "edit" else "Your answer (final)"
                    note = st.text_area(
                        label,
                        key=f"note-{item_id}",
                        value=item.proposed_action if decision == "edit" else "",
                    )

                resolve_disabled = reviewer_identity is None
                if st.button("Resolve Item", key=f"res-{item_id}", disabled=resolve_disabled):
                    try:
                        res = queue.resolve(
                            item_id,
                            decision,
                            approver=reviewer_identity or "unknown",
                            note=note,
                        )
                    except ValueError as exc:
                        st.error(str(exc))
                    else:
                        st.session_state["last_resolution"] = {
                            "task_id": res.task_id,
                            "decision": res.decision,
                            "approver": res.approver,
                            "timestamp": res.timestamp,
                            "final_result": res.final_result,
                        }
                        st.success(f"Resolved `{item_id}` as `{decision}`.")
                        st.rerun()

    if "last_resolution" in st.session_state:
        st.subheader("Last Recorded Resolution")
        st.json(st.session_state["last_resolution"], expanded=False)

    with st.expander("Tamper-Evident Audit Chain", expanded=False):
        intact, broken_at = AuditChain().verify()
        if intact:
            st.success("✅ Audit chain verified — cryptographic integrity confirmed.")
        else:
            st.error(f"⚠️ Audit chain broken at record #{broken_at}.")
        st.json(queue.audit_log(), expanded=False)
