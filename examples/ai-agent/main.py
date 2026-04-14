"""AI Agent — durable content generation with checkpointed LLM calls."""

from __future__ import annotations

import asyncio
import random

from gravtory import Gravtory, step


_DB = __import__("pathlib").Path(__file__).parent / "ai_agent.db"
grav = Gravtory(f"sqlite:///{_DB}")


# Simulated LLM calls (replace with real API calls in production)
async def simulate_llm(prompt: str, model: str = "gpt-4") -> str:
    """Simulate an LLM API call with realistic latency."""
    await asyncio.sleep(0.1)
    return f"[{model} response to: {prompt[:50]}...]"


@grav.workflow(id="content-{task_id}")
class ContentAgent:
    """Research → Outline → Write Sections → Review."""

    @step(1)
    async def research(self, task_id: str, topic: str = "Python workflows") -> dict:
        print(f"[content-{task_id}] Step 1: Researching '{topic}'...")
        findings = await simulate_llm(f"Research the topic: {topic}")
        sources = [
            {"title": "Source A", "url": "https://example.com/a", "relevance": 0.95},
            {"title": "Source B", "url": "https://example.com/b", "relevance": 0.87},
            {"title": "Source C", "url": "https://example.com/c", "relevance": 0.82},
        ]
        print(f"[content-{task_id}] Step 1: Found {len(sources)} relevant sources")
        return {"findings": findings, "sources": sources, "topic": topic}

    @step(2, depends_on=1)
    async def generate_outline(self, task_id: str) -> dict:
        print(f"[content-{task_id}] Step 2: Generating outline...")
        outline = await simulate_llm("Generate an article outline")
        sections = [
            {"title": "Introduction", "word_target": 200},
            {"title": "Core Concepts", "word_target": 400},
            {"title": "Implementation", "word_target": 500},
            {"title": "Best Practices", "word_target": 300},
            {"title": "Conclusion", "word_target": 150},
        ]
        print(f"[content-{task_id}] Step 2: Outline with {len(sections)} sections")
        return {"outline": outline, "sections": sections}

    @step(3, depends_on=2)
    async def write_sections(self, task_id: str) -> dict:
        print(f"[content-{task_id}] Step 3: Writing sections...")
        written = []
        section_titles = [
            "Introduction", "Core Concepts", "Implementation",
            "Best Practices", "Conclusion",
        ]
        for title in section_titles:
            content = await simulate_llm(f"Write section: {title}")
            word_count = random.randint(150, 500)
            written.append({"title": title, "content": content, "words": word_count})
            print(f"[content-{task_id}]   - Wrote '{title}' ({word_count} words)")
        total_words = sum(s["words"] for s in written)
        print(f"[content-{task_id}] Step 3: All sections written ({total_words} words)")
        return {"sections": written, "total_words": total_words}

    @step(4, depends_on=3)
    async def review_and_edit(self, task_id: str) -> dict:
        print(f"[content-{task_id}] Step 4: Reviewing and editing...")
        review = await simulate_llm("Review and suggest edits for the article")
        print(f"[content-{task_id}] Step 4: Review complete — ready to publish")
        return {"review": review, "status": "ready_to_publish", "quality_score": 0.92}


async def main() -> None:
    await grav.start()
    try:
        result = await grav.run(ContentAgent, task_id="article_001")
        print(f"[content-article_001] Workflow completed: {result.status.value}")
    finally:
        await grav.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
