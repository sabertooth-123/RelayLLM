//! Durable workflows (v0.2): definitions, event history, and the pure
//! fold/decide functions. See docs/WORKFLOWS_DESIGN.md.
//!
//! Like the rest of `relay-core`, this module does no I/O and reads no
//! clock: the coordinator passes `now` in and persists everything that
//! comes out, so deciding over the same history always gives the same
//! answer.

pub mod decide;
pub mod definition;
pub mod history;
pub mod run;
pub mod view;

pub use decide::{Command, DecideContext, DecideError, Decision, decide};
pub use definition::{
    Activity, ActivityDefinition, BranchDefinition, DefinitionError, INPUT_ADDRESS,
    INPUTS_PAYLOAD_KEY, Step, StepBody, StepDefinition, Workflow, WorkflowDefinition,
    WorkflowLimits,
};
pub use history::{
    ActivityState, ActivityStatus, ColumnsError, HistoryError, RunState, RunStatus, TimerState,
    WorkflowEvent, WorkflowEventKind, fold,
};
pub use run::{
    NewWorkflow, ParseWorkflowIdError, ParseWorkflowStateError, StartError, StartWorkflowRequest,
    WorkflowId, WorkflowState,
};
pub use view::{
    ActivityView, ActivityViewStatus, CancelWorkflowRequest, JobProgress, RunMeta, SignalRequest,
    SignalResponse, StartWorkflowResponse, StepKind, StepStatus, StepView, WorkflowHistoryView,
    WorkflowView, build_view,
};

#[cfg(test)]
mod tests;
