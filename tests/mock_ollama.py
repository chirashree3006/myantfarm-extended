"""
Mock Ollama server for testing the whole stack WITHOUT a model.

It imitates TinyLlama's typical behaviour and cost:
  * vague prose for the big single-agent prompt
  * semi-specific numbered steps for the paper's planner prompt
  * mostly-valid JSON (sometimes wrapped in chatter) for narrow agent prompts
  * CPU-bound timing: MOCK_MS_PER_TOKEN per generated token, at most
    MOCK_PARALLEL generations at a time (like OLLAMA_NUM_PARALLEL), so
    load tests queue the way a real CPU model does.

    uvicorn tests.mock_ollama:app --port 11434
"""

import asyncio
import json
import os
import random
import re

from fastapi import FastAPI, Request

app = FastAPI()
MS_PER_TOKEN = float(os.getenv("MOCK_MS_PER_TOKEN", "4"))
PARALLEL = asyncio.Semaphore(int(os.getenv("MOCK_PARALLEL", "2")))
NAME = os.getenv("INSTANCE_ID", "mock-ollama")


@app.get("/api/tags")
async def tags():
    return {"models": [{"name": "tinyllama:latest"}]}


def respond(p: str, rnd: random.Random) -> str:
    if "What is happening and what should we do?" in p:
        return ("Based on the incident report, it seems that there are some issues with the system. "
                "The team should investigate further and check recent changes. It is also recommended "
                "to monitor the situation and contact the support team if the problem continues.")
    if p.rstrip().endswith("Assistant:"):   # single-agent follow-up chat
        return rnd.choice([
            "It looks like the servers may be under heavy load. You could try checking the logs and restarting "
            "the affected services. It may also help to scale up resources if the issue continues.",
            "I'm not able to see which replica exactly, but the errors suggest a capacity problem. Consider "
            "reviewing the configuration and monitoring the system closely.",
            "You may want to increase the resources available to the web servers. Please check with the "
            "infrastructure team about the right values for your environment."])
    if "Remediation Planner" in p:
        return (" Check the logs of the affected service to find the error.\n"
                "2. Roll back the deployment to the previous version.\n"
                "3. Monitor the database connections and restart the server if needed.")
    if "Risk Assessor" in p:
        return "Rolling back may cause brief downtime. Mitigate by doing it during low traffic."
    if p.startswith("You are the Diagnosis Specialist"):
        return ("The root cause appears to be a problem with the service after the recent deployment, "
                "which is causing errors and high database usage.")
    if '"summary"' in p:
        m = re.search(r"Root cause: (.+?)\.\n", p)
        first = re.search(r"First action: (.+)", p)
        text = json.dumps({"summary": f"The incident is caused by {(m.group(1) if m else 'an issue').lower()}. "
                                      f"First, {first.group(1) if first else 'investigate'}"})
        if rnd.random() < 0.2:
            text = "Sure! Here is the summary: " + json.dumps({"summary": "There is an incident."})
        return text
    facts = re.findall(r"^- (.+)$", p, re.M)
    obs = facts[0][:140] if facts and "no errors" not in facts[0] else "No anomalies in the data."
    text = json.dumps({"observation": f"The data shows {obs}", "severity": "high" if facts else "none"})
    if rnd.random() < 0.25:
        text = "Here is the JSON you asked for:\n" + text + "\nI hope this helps!"
    return text


@app.post("/api/generate")
async def generate(req: Request):
    body = await req.json()
    p = body.get("prompt", "")
    rnd = random.Random(hash(p) & 0xFFFF)
    text = respond(p, rnd)
    n_tokens = min(int(body.get("options", {}).get("num_predict", 200)), max(20, len(text) // 4))
    async with PARALLEL:
        await asyncio.sleep(MS_PER_TOKEN * (n_tokens + len(p) // 40) / 1000)
    return {"model": body.get("model"), "response": text, "done": True,
            "prompt_eval_count": len(p) // 4, "eval_count": n_tokens, "served_by": NAME}
