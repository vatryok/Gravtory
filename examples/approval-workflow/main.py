"""Approval Workflow — human-in-the-loop expense approval with signals."""

from __future__ import annotations

import asyncio

from gravtory import Gravtory, step


_DB = __import__("pathlib").Path(__file__).parent / "approval_workflow.db"
grav = Gravtory(f"sqlite:///{_DB}")


@grav.workflow(id="expense-{expense_id}")
class ExpenseWorkflow:
    """Submit expense → wait for approval → process or reject."""

    @step(1)
    async def submit_expense(
        self, expense_id: str, amount: float = 250.0, description: str = "Team lunch"
    ) -> dict:
        print(f"[expense-{expense_id}] Step 1: Submitting expense ${amount} — {description}")
        await asyncio.sleep(0.05)
        print(f"[expense-{expense_id}] Step 1: Expense submitted, awaiting approval")
        return {"expense_id": expense_id, "amount": amount, "description": description}

    @step(2, depends_on=1)
    async def review_decision(self, expense_id: str) -> dict:
        # In a real app, this step would use @wait_for_signal("approval")
        # For this demo, we simulate an auto-approval
        print(f"[expense-{expense_id}] Step 2: Reviewing expense...")
        await asyncio.sleep(0.1)
        approved = True  # Simulated approval
        reviewer = "manager@example.com"
        print(f"[expense-{expense_id}] Step 2: {'Approved' if approved else 'Rejected'} by {reviewer}")
        return {"approved": approved, "reviewer": reviewer}

    @step(3, depends_on=2)
    async def process_result(self, expense_id: str) -> dict:
        print(f"[expense-{expense_id}] Step 3: Processing reimbursement...")
        await asyncio.sleep(0.05)
        print(f"[expense-{expense_id}] Step 3: Reimbursement processed!")
        return {"reimbursed": True}


async def main() -> None:
    await grav.start()
    try:
        result = await grav.run(ExpenseWorkflow, expense_id="exp_001")
        print(f"[expense-exp_001] Workflow completed: {result.status.value}")
    finally:
        await grav.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
