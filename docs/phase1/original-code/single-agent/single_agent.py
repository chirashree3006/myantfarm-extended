"""
Single-agent baseline: the "copilot" condition from the reference paper.

This pulls all four data streams from the demo website, mashes them into
ONE shared incident description (exactly like the paper's single-agent
condition), and asks a single LLM call to diagnose it. Expect a vague,
generic answer -- that vagueness IS the result you're demonstrating.

Prerequisites:
    - demo-website running at http://localhost:8080  (uvicorn app:app ...)
    - Ollama running with tinyllama pulled            (ollama pull tinyllama)

Run:
    python single_agent.py
"""

import json
import urllib.request

DEMO_SITE = "http://localhost:8080"
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "tinyllama"


def fetch(path: str):
    with urllib.request.urlopen(f"{DEMO_SITE}{path}") as resp:
        return json.loads(resp.read())


def build_incident_description() -> str:
    """Combine all four streams into one shared text block -- this mirrors
    exactly what the paper's single-agent condition receives."""
    frontend = fetch("/logs/frontend?limit=5")
    server = fetch("/logs/server?limit=5")
    db = fetch("/metrics/db?limit=3")
    reports = fetch("/reports/users?limit=3")
    status = fetch("/admin/status")

    parts = [
        "INCIDENT REPORT",
        f"Deploy version: {status.get('deploy_version')} "
        f"(deployed {status.get('deploy_time')})",
        f"Total requests: {status.get('total_requests')}, "
        f"failures: {status.get('total_failures')}",
        "",
        "Frontend errors (browser side):",
        *([f"  - {e['message']}" for e in frontend] or ["  (none)"]),
        "",
        "Server logs (backend):",
        *([f"  - [{e['level']}] {e['message']}" for e in server] or ["  (none)"]),
        "",
        "Database metrics:",
        *(
            [
                f"  - {m['active_connections']}/{m['pool_size']} connections in use "
                f"({m['pool_utilization_pct']}% utilization, "
                f"{m['avg_wait_time_ms']}ms avg wait)"
                for m in db
            ]
            or ["  (none)"]
        ),
        "",
        "User support tickets:",
        *([f"  - {r['text']}" for r in reports] or ["  (none)"]),
    ]
    return "\n".join(parts)


def ask_single_agent(incident_text: str) -> str:
    prompt = (
        "You are an on-call engineer assistant. Analyze the incident below "
        "and tell the team what is wrong and what to do about it.\n\n"
        f"{incident_text}\n\n"
        "What is happening and what should we do?"
    )

    payload = json.dumps(
        {"model": MODEL, "prompt": prompt, "stream": False}
    ).encode()

    req = urllib.request.Request(
        OLLAMA_URL, data=payload, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.loads(resp.read())
    return result.get("response", "").strip()


if __name__ == "__main__":
    print("=" * 60)
    print("Fetching incident data from demo website...")
    print("=" * 60)
    incident = build_incident_description()
    print(incident)

    print("\n" + "=" * 60)
    print("Asking single agent (TinyLlama) to diagnose it...")
    print("=" * 60)
    answer = ask_single_agent(incident)
    print(answer)

    print("\n" + "=" * 60)
    print("Look at the answer above: is it specific (an exact command,")
    print("an exact root cause) or vague (\"investigate further\",")
    print("\"check recent changes\")? That's your single-agent baseline result.")
    print("=" * 60)
