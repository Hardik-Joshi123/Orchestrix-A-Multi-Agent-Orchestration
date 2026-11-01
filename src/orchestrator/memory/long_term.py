"""Long-term semantic memory in ChromaDB.

Stores what worked and which tools were used, retrievable by semantic similarity.
Degrades to an in-process keyword store when ChromaDB is unavailable.
"""

from __future__ import annotations

import hashlib
import re

from ..config import get_settings

_COLLECTION = "agent_memories"
# Module-level fallback store so memories survive within a process run.
_LOCAL: list[dict] = []
_CACHED_COLLECTION = None
_CONNECTION_ATTEMPTED = False


def reset_local_store() -> None:
    """Helper for test isolation."""
    global _LOCAL, _CACHED_COLLECTION, _CONNECTION_ATTEMPTED
    _LOCAL.clear()
    _CACHED_COLLECTION = None
    _CONNECTION_ATTEMPTED = False


class LongTermMemory:
    def __init__(self) -> None:
        self._collection = self._connect()

    def _connect(self):
        global _CACHED_COLLECTION, _CONNECTION_ATTEMPTED
        if _CONNECTION_ATTEMPTED:
            return _CACHED_COLLECTION
        _CONNECTION_ATTEMPTED = True
        try:
            import socket

            import chromadb

            settings = get_settings()
            with socket.create_connection((settings.chroma_host, settings.chroma_port), timeout=0.2):
                pass
            client = chromadb.HttpClient(host=settings.chroma_host, port=settings.chroma_port)
            _CACHED_COLLECTION = client.get_or_create_collection(_COLLECTION)
        except Exception:
            _CACHED_COLLECTION = None
        return _CACHED_COLLECTION

    def remember(self, task: str, outcome: str, metadata: dict | None = None) -> None:
        meta = {"task": task, **(metadata or {})}
        doc = f"TASK: {task}\nOUTCOME: {outcome}"
        if self._collection is not None:
            try:
                mid = f"mem-{hashlib.sha256(doc.encode('utf-8')).hexdigest()[:32]}"
                if hasattr(self._collection, "upsert"):
                    self._collection.upsert(ids=[mid], documents=[doc], metadatas=[meta])
                else:
                    try:
                        self._collection.add(ids=[mid], documents=[doc], metadatas=[meta])
                    except Exception:
                        self._collection.update(ids=[mid], documents=[doc], metadatas=[meta])
                return
            except Exception:
                pass
        # Deduplication in in-process fallback
        for m in _LOCAL:
            if m.get("doc") == doc:
                m["meta"].update(meta)
                return
        _LOCAL.append({"doc": doc, "meta": meta})

    def recall(self, query: str, k: int = 3) -> list[str]:
        if self._collection is not None:
            try:
                res = self._collection.query(query_texts=[query], n_results=k)
                docs = res.get("documents", [[]])[0]
                if docs:
                    return docs
            except Exception:
                pass
        # Keyword fallback ranking with token extraction.
        terms = set(re.findall(r"\w+", query.lower()))
        if not terms:
            return []
        scored = sorted(
            _LOCAL,
            key=lambda m: len(terms & set(re.findall(r"\w+", m["doc"].lower()))),
            reverse=True,
        )
        return [m["doc"] for m in scored[:k] if terms & set(re.findall(r"\w+", m["doc"].lower()))]
