# Errors API Reference

All Gravtory exceptions inherit from `GravtoryError`, so you can catch
everything with a single `except GravtoryError` clause if desired.

## Error Hierarchy

```
GravtoryError
├── WorkflowNotFoundError
├── WorkflowAlreadyExistsError
├── WorkflowRunNotFoundError
├── WorkflowRunAlreadyExistsError
├── WorkflowCancelledError
├── WorkflowDeadlineExceededError
├── WorkflowDeadlockError
├── StepError
│   ├── StepTimeoutError
│   ├── StepRetryExhaustedError (alias: StepExhaustedError)
│   ├── StepDependencyError
│   ├── StepConditionError
│   ├── StepAbortError
│   └── StepOutputTypeError
├── CompensationError
├── BackendError
│   ├── BackendConnectionError
│   ├── BackendMigrationError
│   └── BackendLockError
├── SerializationError
├── SignalError
│   └── SignalTimeoutError
├── CircuitOpenError
├── ConcurrencyLimitError
├── ValidationError
└── ConfigurationError
    └── InvalidWorkflowError
```

---

::: gravtory.core.errors
    options:
      show_root_heading: false
      members_order: source
