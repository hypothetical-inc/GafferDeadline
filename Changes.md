# 0.57.0.0

- Added controls for Gaffer's `-threads` command line parameter.
  - The GafferDeadline dispatcher plug `threads` can be used to control the thread count on a per job basis. If `threads` is set to 0, it will be ignored when executing the Deadline job.
  - GafferDeadline will also recognize a Deadline Worker's `CpuAffinity` property, if set, and use the number of enabled CPUs as the thread count.
  - If the `threads` plug is non-zero and a Worker has its `CpuAffinity` property enabled, Gaffer will use the lesser of the two values as its thread count.

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