# GafferDeadline #
Deadline Dispatcher for Gaffer. There are three components - the Gaffer dispatcher, Deadline plugin and Python dependency script called by Deadline to check for task dependencies getting released.

Gaffer can generate arbitrarily complex node trees with dependencies between Task Nodes that are more complicated than Deadline supports through it's standard job and task dependencies. To support Gaffer's DAG style of dependencies, a dependency script is included. Deadline runs this script periodically for each job to determine which, if any, tasks for that job are ready to be released. Using the included dispatcher and plugin this should be transparent to the user once everything is setup as described below.

GafferDeadline will auto-detect the most efficient method of setting up dependencies by default (see not in Usage about overriding this behavior). Most Gaffer scripts will likely be submitted with frame-to-frame dependencies, but it will fall back to the scripted task dependency system or job-to-job on a per-node basis if it doesn't fit within Deadline's dependency scheme.

It is tested on Linux and the beta Windows Gaffer build. OS X compatibility is unknown.

## Installing ##
1. Extract the archive / clone the repository to a directory accessible to Gaffer.
2. Move the "Gaffer" subdirectory to your Deadline repository "custom/plugins" directory. This is the Deadline plugin that will run Gaffer jobs on your render farm.
3. Add the directory where you extracted / cloned the repository to the GAFFER_EXTENSION_PATHS environment variable before running Gaffer.
4. Move the gaffer_batch_dependency.py file to a location where all of your Deadline Slaves and Pulse machines can access the file. Deadline will run that script according to your repository settings to check for tasks that can be released from pending status based on their dependencies being completed.
If you have multiple operating systems in your Deadline installation, you will likely need to set up path mapping for machines to locate the script.
5. Set the DEADLINE_DEPENDENCY_SCRIPT_PATH environment variable to the full path (including filename) where you saved the gaffer_batch_dependency.py file before running Gaffer. GafferDeadline dispatcher uses this variable as the location for the dependency script when submitting jobs to Deadline.
6. Ensure that the DEADLINE_PATH environment variable is set to the directory where the "deadlinecommand" executable lives. This is typically set system-wide when you install the Deadline Client. GafferDeadline uses this environment variable to locate "deadlinecommand" for interacting with your Deadline repository.

## Using ##
With everything set up correctly, Task Nodes in Gaffer will have a Deadline section on their Dispatcher tab. This section is where you setup the Deadline configuration for that task. You can set most common settings like groups, pools, priority, description, etc.

When you are ready to submit the node(s) press the node's "Execute" button and select Deadline from the dropdown box of available dispatchers.

You need a Deadline Client installed and connected to your repository on the machines you will be running GafferDeadline from. GafferDeadline uses the Deadline installation on the host machine, similar to other integrated submitters Deadline includes such as for Nuke, Houdini, etc.

The Deadline settings in Gaffer include an override for the dependency method for that node. This override controls downstream Task Nodes that depend on the node on which it was set. Most of the time it should be left on Auto to let the dispatcher determine the most efficient method. If you know a node needs to be handled in a particular way, you can force its dendency method with the override plug. Usually the "Full Job" setting will be the safest but least flexible because downstream tasks will wait for all frames of that job to complete before being released.

## Running Unit Tests ##
You don't need to run the unit tests for normal use of GafferDeadline, but if you want to make customizations it is recommended that you add unit tests as appropriate and run the existing tests to ensure compatibility.

To run the unit tests, you need to have an installation of Gaffer and have your Python environment setup to point to that installation. The easiest way to do that is to use the included gaffer_env (Linux) and gaffer_env.bat (Windows) files to setup the environment first. Then you can use regular Python unit test runners to run tests.

More specifically:
1. PATH environment variable needs to include the gaffer/bin, gaffer/lib (on Windows) and gaffer/python directories.
2. PYTHONPATH environment variable needs to include the gaffer/python directory.
3. On Linux the LD_LIBRARY_PATH needs to be set to the gaffer/lib directory.

There is also a Visual Studio Code environment included that may be helpful.

## Contributing ##
Feedback and pull requests are welcome! If you have ideas about how to improve the dispatcher, find bugs or would like to submit improvements, please create an issue on GitHub for discussion or a pull request.

## Copyright and License ##

© 2019 Hypothetical Inc. All rights reserved.

Distributed under the [BSD license](LICENSE).