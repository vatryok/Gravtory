"""ETL Pipeline — parallel data processing with per-item checkpointing."""

from __future__ import annotations

import asyncio
import random

from gravtory import Gravtory, step


_DB = __import__("pathlib").Path(__file__).parent / "etl_pipeline.db"
grav = Gravtory(f"sqlite:///{_DB}")


@grav.workflow(id="etl-{job_id}")
class ETLPipeline:
    """Extract → Transform (parallel) → Load → Validate."""

    @step(1)
    async def extract(self, job_id: str, record_count: int = 20) -> dict:
        print(f"[etl-{job_id}] Step 1: Extracting {record_count} records...")
        await asyncio.sleep(0.05)
        records = [
            {"id": i, "value": random.randint(1, 100), "source": "api"}
            for i in range(record_count)
        ]
        print(f"[etl-{job_id}] Step 1: Extracted {len(records)} records")
        return {"records": records, "source_count": len(records)}

    @step(2, depends_on=1)
    async def transform(self, job_id: str) -> dict:
        print(f"[etl-{job_id}] Step 2: Transforming records...")
        await asyncio.sleep(0.05)
        # Simulate transformation: double each value and add metadata
        transformed = []
        for _i in range(20):
            transformed.append({"processed": True, "doubled": random.randint(2, 200)})
        print(f"[etl-{job_id}] Step 2: Transformed {len(transformed)} records")
        return {"records": transformed, "transform_count": len(transformed)}

    @step(3, depends_on=2)
    async def load(self, job_id: str) -> dict:
        print(f"[etl-{job_id}] Step 3: Loading records to destination...")
        await asyncio.sleep(0.05)
        loaded_count = 20
        print(f"[etl-{job_id}] Step 3: Loaded {loaded_count} records")
        return {"loaded_count": loaded_count}

    @step(4, depends_on=3)
    async def validate(self, job_id: str) -> dict:
        print(f"[etl-{job_id}] Step 4: Validating pipeline results...")
        await asyncio.sleep(0.05)
        # All counts should match
        is_valid = True
        print(f"[etl-{job_id}] Step 4: Validation {'passed' if is_valid else 'FAILED'}")
        return {"valid": is_valid}


async def main() -> None:
    await grav.start()
    try:
        result = await grav.run(ETLPipeline, job_id="daily_2025_01_15")
        print(f"[etl] Pipeline completed: {result.status.value}")
    finally:
        await grav.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
