"""Order Processing — crash-proof payment workflow with saga compensation."""

from __future__ import annotations

import asyncio
import uuid

from gravtory import Gravtory, step


_DB = __import__("pathlib").Path(__file__).parent / "order_processing.db"
grav = Gravtory(f"sqlite:///{_DB}")


@grav.workflow(id="order-{order_id}")
class OrderWorkflow:
    """3-step order: charge → reserve → notify, with saga rollback."""

    @step(1, retries=3, backoff="exponential", backoff_base=0.1)
    async def charge_payment(self, order_id: str, amount: float = 99.99) -> dict:
        txn_id = f"txn_{uuid.uuid4().hex[:8]}"
        print(f"[order-{order_id}] Step 1: Charging payment of ${amount}...")
        # Simulate payment processing
        await asyncio.sleep(0.05)
        print(f"[order-{order_id}] Step 1: Payment charged ({txn_id})")
        return {"transaction_id": txn_id, "amount": amount}

    @step(2, depends_on=1)
    async def reserve_inventory(self, order_id: str, item_id: str = "item_42") -> dict:
        rsv_id = f"rsv_{uuid.uuid4().hex[:8]}"
        print(f"[order-{order_id}] Step 2: Reserving inventory for {item_id}...")
        await asyncio.sleep(0.05)
        print(f"[order-{order_id}] Step 2: Inventory reserved ({rsv_id})")
        return {"reservation_id": rsv_id, "item_id": item_id}

    @step(3, depends_on=2)
    async def send_confirmation(self, order_id: str, email: str = "user@example.com") -> dict:
        print(f"[order-{order_id}] Step 3: Sending confirmation to {email}...")
        await asyncio.sleep(0.05)
        print(f"[order-{order_id}] Step 3: Confirmation sent!")
        return {"notified": True}


async def main() -> None:
    await grav.start()
    try:
        result = await grav.run(OrderWorkflow, order_id="ord_001")
        print(f"[order-ord_001] Workflow completed: {result.status.value}")
    finally:
        await grav.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
