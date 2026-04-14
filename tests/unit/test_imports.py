"""Verify that all public API imports work correctly."""

import gravtory


class TestPublicAPI:
    def test_version_exists(self) -> None:
        assert hasattr(gravtory, "__version__")
        assert isinstance(gravtory.__version__, str)
        assert gravtory.__version__ == "0.1.0"

    def test_all_exports_exist(self) -> None:
        for name in gravtory.__all__:
            assert hasattr(gravtory, name), f"Missing export: {name}"

    def test_enum_imports(self) -> None:
        assert gravtory.WorkflowStatus.PENDING == "pending"
        assert gravtory.StepStatus.COMPLETED == "completed"
        assert gravtory.WorkerStatus.ACTIVE == "active"
        assert gravtory.ScheduleType.CRON == "cron"

    def test_type_imports(self) -> None:
        run = gravtory.WorkflowRun(id="test", workflow_name="wf")
        assert run.id == "test"

    def test_error_imports(self) -> None:
        assert issubclass(gravtory.WorkflowNotFoundError, gravtory.GravtoryError)
        assert issubclass(gravtory.StepError, gravtory.GravtoryError)
        assert issubclass(gravtory.BackendError, gravtory.GravtoryError)

    def test_backend_importable(self) -> None:
        from gravtory.backends.base import Backend

        assert Backend is not None
