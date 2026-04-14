"""FastAPI endpoint for sending approval signals to running workflows."""

from __future__ import annotations

from fastapi import FastAPI
from pydantic import BaseModel

from gravtory import Gravtory

app = FastAPI(title="Expense Approval API")
_DB = __import__("pathlib").Path(__file__).parent / "approval_workflow.db"
grav = Gravtory(f"sqlite:///{_DB}")


class ApprovalRequest(BaseModel):
    approved: bool
    reviewer: str


@app.on_event("startup")
async def startup() -> None:
    await grav.start()


@app.on_event("shutdown")
async def shutdown() -> None:
    await grav.shutdown()


@app.post("/approve/{expense_id}")
async def approve_expense(expense_id: str, request: ApprovalRequest) -> dict:
    """Send an approval signal to a waiting expense workflow."""
    await grav.signal(
        f"expense-{expense_id}",
        "approval",
        {"approved": request.approved, "reviewer": request.reviewer},
    )
    return {"status": "signal_sent", "expense_id": expense_id}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
