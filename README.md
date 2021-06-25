# GafferDeadline #
Deadline Dispatcher for Gaffer. There are three components - the Gaffer dispatcher, Deadline plugin and Python dependency script called by Deadline to check for task dependencies getting released.

Gaffer can generate arbitrarily complex node trees with dependencies between Task Nodes that are more complicated than Deadline supports through it's standard job and task dependencies. To support Gaffer's DAG style of dependencies, a dependency script is included. Deadline runs this script periodically for each job to determine which, if any, tasks for that job are ready to be released. Using the included dispatcher and plugin this should be transparent to the user once everything is setup as described below.

GafferDeadline will auto-detect the most efficient method of setting up dependencies by default (see not in Usage about overriding this behavior). Most Gaffer scripts will likely be submitted with frame-to-frame dependencies, but it will fall back to the scripted task dependency system or job-to-job on a per-node basis if it doesn't fit within Deadline's dependency scheme.

It is tested on Linux and the beta Windows Gaffer build. OS X compatibility is unknown.

## Installing ##
1. Extract the archive / clone the repository to a directory accessible to Gaffer.
2. Move the `Gaffer` subdirectory to your Deadline repository `custom/plugins` directory. This is the Deadline plugin that will run Gaffer jobs on your render farm.
3. Add the directory where you extracted / cloned the repository to the `GAFFER_EXTENSION_PATHS` environment variable before running Gaffer.
4. Move the `gaffer_batch_dependency.py` file to a location where all of your Deadline Workers and Pulse machines can access the file. Deadline will run that script according to your repository settings to check for tasks that can be released from pending status based on their dependencies being completed.
If you have multiple operating systems in your Deadline installation, you will likely need to set up path mapping for machines to locate the script.
5. Set the `DEADLINE_DEPENDENCY_SCRIPT_PATH` environment variable to the full path (including filename) where you saved the `gaffer_batch_dependency.py` file before running Gaffer. GafferDeadline dispatcher uses this variable as the location for the dependency script when submitting jobs to Deadline.
6. Ensure that the `DEADLINE_PATH` environment variable is set to the directory where the `deadlinecommand` executable lives. This is typically set system-wide when you install the Deadline Client. GafferDeadline uses this environment variable to locate `deadlinecommand` for interacting with your Deadline repository.

## Deadline Setup ##
Once you have the Gaffer plugin copied to your Deadline `custom/plugins` directory you need to setup the Gaffer plugin in Deadline. In Super-User Mode in Deadline Monitor, go to Tools -> Configure Plugins. Select Gaffer from the list and add the paths to your Gaffer executables as they are found on your workstations and rendering machines. To support more than one Gaffer version in a single studio, GafferDeadline adds the version of Gaffer that is submitting the job to the Deadline Job settings. The Gaffer plugin looks for an executable of the same version and will fail if it is not found. If you are using a Gaffer version that GafferDeadline is not configured for, it's easy to add that version:
1. Open the `gaffer.param` file from your `<Deadline Repository>/custom/plugins/gaffer` directory.
2. Copy and paste the block starting with `[Executable0_53_0_0]` (or similar) to add a new entry.
3. Change the version number to the exact version of Gaffer you are running: `[Executable0_51_0_1]` corresponds to Gaffer version 0.51.0.1.
4. Save the file and open up Configure Plugins again and the new version will be available to set the Gaffer binary locations.

## Using ##
With everything set up correctly, Task Nodes in Gaffer will have a Deadline section on their Dispatcher tab. This section is where you setup the Deadline configuration for that task. You can set most common settings like groups, pools, priority, description, etc.

When you are ready to submit the node(s) press the node's "Execute" button and select Deadline from the dropdown box of available dispatchers.

You need a Deadline Client installed and connected to your repository on the machines you will be running GafferDeadline from. GafferDeadline uses the Deadline installation on the host machine, similar to other integrated submitters Deadline includes for Nuke, Houdini, etc.

The Deadline settings in Gaffer include an override for the dependency method for that node. This override controls downstream Task Nodes that depend on the node on which it was set. Most of the time it should be left on "Auto" to let the dispatcher determine the most efficient method. If you know a node needs to be handled in a particular way, you can force its dependency method with the override plug. Usually the "Full Job" setting will be the safest but least flexible because downstream tasks will wait for all frames of that job to complete before being released.

### Auxiliary Files ###
GafferDeadline can submit auxiliary files along with the dispatched job at submission time. These files will be uploaded to the Deadline repository and downloaded to each Deadline Worker when it dequeues a task.

The GafferDeadline Deadline plugin sets an environment variable called `AUXFILEDIRECTORY` to the local directory on the Deadline Worker where the files are downloaded. This environment variable can then be used in any of Gaffer's usual string substitutions to point files to the auxiliary file location.

### Render Threads and GPU Affinity ###
GafferDeadline sets the environment variable `CPUTHREAD` the Deadline Worker's render thread. GafferDeadline also sets the `GPUAFFINITY` environment variable to a comma-separated list of GPU Threads configured for that Worker. More information on setting up GPU Affinity can be found at https://www.awsthinkbox.com/blog/cpu-and-gpu-affinity-in-deadline.

## Running Unit Tests ##
You don't need to run the unit tests for normal use of GafferDeadline, but if you want to make customizations it is recommended that you add unit tests as appropriate and run the existing tests to ensure compatibility.

To run the unit tests, you need to have an installation of Gaffer.
- All OS : set the `GAFFER_EXTENSION_PATHS` environment variable to the directory for GafferDeadline.
- Linux : set the `GAFFER_ROOT` environment variable to your Gaffer installation directory. From the GAFFER_ROOT/bin directory, run `./gaffer test GafferDeadlineTest GafferDeadlineUITest`
- Windows : You don't need to set the GAFFER_ROOT environment variable. From your Gaffer installation "bin" subdirectory, run `gaffer.bat test GafferDeadlineTest GafferDeadlineUITest`

There is also a Visual Studio Code environment included that may be helpful.

### Testing on Python Versions Lower than 3.3 ###
GafferDeadline uses the `mock` library to avoid sending actual jobs to Deadline. If you are testing on with Python versions lower than 3.3 you will need to install `mock` for the Python distribution for Gaffer. This can be done by running
`gaffer env python -m pip install mock`

## Contributing ##
Feedback and pull requests are welcome! If you have ideas about how to improve the dispatcher, find bugs or would like to submit improvements, please create an issue on GitHub for discussion or a pull request.

## Copyright and License ##

© 2019 Hypothetical Inc. All rights reserved.

Distributed under the [BSD license](LICENSE).