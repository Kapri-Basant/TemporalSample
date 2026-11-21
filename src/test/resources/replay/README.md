# Reviewed workflow history

`JobWorkflowV1-success.json` is the first reviewed V1 baseline. It contains only fake test data, a saved wait, and successful task results. It is not a history from an older production release.

The fixed-fixture test replays this file against the current V1 workflow code. A failure can mean a code change would break jobs that started with this version.

Do not replace the fixture just to make a test pass. Review the change first. Keep V1 compatible, or introduce a new workflow type/version and keep workers that can finish V1 jobs.

The success test exports fresh history to `build/replay/` for review. It never overwrites this source fixture. Add reviewed failure, cancellation, and actual prior-release histories before a production upgrade; this small baseline does not cover every path.
