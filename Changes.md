# 0.58.0.0

- Add menu entry `/Dispatch/Deadline Dispatch` for compatibility with Gaffer 1.4.
- *Breaking change* : Changed the naming of the temporary files created at submission time to send settings to Deadline. Files are now named by the hash of the task node.
- Fixed error `Context has no variable named "frame"` when dispatching with `DeadlineDispatch`.
- *Breaking change* : Changed the API for `GafferDeadlineJob.submitJob()`. It now takes a single directory where the job and plugin submission files will be saved. 
- *Breaking change* : Temporary job submission files are given random names by Python's `tempfile` module instead of attempting to use the hash of the dispatch node.
- API : Added `GafferDeadlineJob.environmentVariables()` method.
- Fixed bug that prevented context variables from being substituted in the `deadlineSettings` and `environmentVariables` plugs.
- Fixed bug that prevented context variables from being substituted into GafferDeadline plugs. (#79)
- Added `extraDeadlineSettings` and `extraEnvironmentVariables` plugs. These can be set by an expression to add arbitrary numbers of Deadline settings and environment variables. Entries in these plugs will take precedence over identically named settings / variables in the `deadlineSettings` and `environmentVariables` plugs.
- *Breaking change* : Changed job names to be `${dispatcher.jobName}.${taskNodeName}`. (#80)
- Added `batchName` plug to allow easy customization of the batch name. Previously it would be set to the dispatcher's `jobName` plug value unless overridden in `deadlineSettings`. The default value is the same as dispatchers' `jobName` default value, so unless you change the `batchName` plug value, batches will be named the same as previously.

- Updated supported Gaffer versions to `1.3.16.7` and `1.4.11.0`.

# 0.57.3.0
- Fixed bug causing an error when dispatching when passing `pathlib.Path` values to `GafferDeadlineJob.setAuxFiles()`.
- Added support for `GafferScene.RenderPassWedge`. This fixes the error `TypeError: unsupported operand type(s) for -: 'NoneType' and 'NoneType'` raised when attempting to dispatch a `RenderPassWedge`.

# 0.57.2.0

- Reduced the severity of the check on no-op node batch size from an error to a warning.
- Removed support for Gaffer versions `1.2.1.0` and `1.2.6.0`
- Added support for Gaffer versions `1.3.7.0` and `1.2.10.5`
- Added support for Arnold progress updates in the Deadline plugin.
- Update Deadline Plugin to use Python 3.
- Update Deadline Plugin to be compatible with Python 3.10.

# 0.57.1.0

- Added `GafferDeadline.DeadlineTask` node. Instead of submitting a `Gaffer` plugin to Deadline, this node will submit any plugin, set by the `plugin` plug. Plugin parameters can be added to the `parameters` plug and will be submitted with the job.

# 0.57.0.0

- Added controls for Gaffer's `-threads` command line parameter.
  - The GafferDeadline dispatcher plug `threads` can be used to control the thread count on a per job basis. If `threads` is set to 0, it will be ignored when executing the Deadline job.
  - GafferDeadline will also recognize a Deadline Worker's `CpuAffinity` property, if set, and use the number of enabled CPUs as the thread count.
  - If the `threads` plug is non-zero and a Worker has its `CpuAffinity` property enabled, Gaffer will use the lesser of the two values as its thread count.
- Added `logLevel` :
  - Added plug to the dispatcher settings to control the `IECORE_LOG_LEVEL` environment variable for submitted jobs.
  - API : Added `GafferDeadlineJob.setLogLevel()` and `GafferDeadline.getLogLevel()` methods.
- Added output support :
  - Added `outputs` plug to GafferDeadline settings. The values of this plug will use all string substitutions _except_ frame substitutions. This allows Deadline to substitute frame numbers itself.
  - API : 
    - Added `GafferDeadlineJob.addOutput()` to add an output with an optional context for substitutions.
    - Added `GafferDeadlineJob.getOutputs()` to return the current job outputs.
    - Added `GafferDeadlineJob.clearOutputs()` to remove all job outputs.

# 0.56.1.0

- Added Alfred style progress (commonly used by Houdini renderers) updates in the Deadline plugin.
- Added support for Gaffer 0.61.1.2, 1.2.0.0a1 and 1.2.0.0a2.
- Added the ability to use `pathlib.Path` objects in auxiliary files. This can be done, for example, in a prespool signal handler that modifies the jobs auxiliary files via `setAuxFiles()`.
- DeadlineDispatcherTest : Removed support for Python 2.

# 0.56.0.1
## Bugs Squished
- Fixed error "This application failed to start because no Qt platform plugin could be initialized." when running `dispatch` app on a headless system.

# 0.56.0.0

## New Features

- Deadline Plugin : Added this Changes.md file to track changes. For previous updates, see the release history at https://github.com/hypothetical-inc/GafferDeadline/releases
- Added support for Gaffer 0.60.7.0 and 0.60.8.0.
- Task Logging : Fixed logging of Gaffer errors. Previously the Gaffer Deadline plugin would terminate at the first line containing "ERROR : ", but Gaffer prints the more interesting error information on subsequent lines.
    
## Breaking Changes
- A task will now **only** error if Gaffer exits with a non-zero exit code. To accomplish this in a PythonCommand, you can `raise RuntimeError("Error Message")` or `assert`. Outputting "ERROR : ", for example from `IECore.msg(IECore.Msg.Level.Error...)`, will no longer fail the task.