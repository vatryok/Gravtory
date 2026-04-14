"""Tests for the @saga decorator."""

from __future__ import annotations

from gravtory.decorators.saga import saga


class TestSagaDecorator:
    def test_saga_marks_class(self) -> None:
        @saga
        class MyWorkflow:
            pass

        assert getattr(MyWorkflow, "__gravtory_saga__", False) is True

    def test_saga_with_parens(self) -> None:
        @saga()
        class MyWorkflow:
            pass

        assert getattr(MyWorkflow, "__gravtory_saga__", False) is True

    def test_saga_returns_same_class(self) -> None:
        class Original:
            pass

        decorated = saga(Original)
        assert decorated is Original

    def test_saga_on_function(self) -> None:
        @saga
        def my_func() -> None:
            pass

        assert getattr(my_func, "__gravtory_saga__", False) is True
