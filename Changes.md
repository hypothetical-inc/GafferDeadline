# 0.56.0.0

## New Features

- Deadline Plugin : Added this Changes.md file to track changes. For previous updates, see the release history at https://github.com/hypothetical-inc/GafferDeadline/releases
- Added support for Gaffer 0.60.7.0 and 0.60.8.0.
- Task Logging : Fixed logging of Gaffer errors. Previously the Gaffer Deadline plugin would terminate at the first line containing "ERROR : ", but Gaffer prints the more interesting error information on subsequent lines.
    
## Breaking Changes
- A task will now **only** error if Gaffer exits with a non-zero exit code. To accomplish this in a PythonCommand, you can `raise RuntimeError("Error Message")` or `assert`. Outputting "ERROR : ", for example from `IECore.msg(IECore.Msg.Level.Error...)`, will no longer fail the task.