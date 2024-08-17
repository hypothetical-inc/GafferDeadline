##########################################################################
#
#  Copyright (c) 2019, Hypothetical Inc. All rights reserved.
#
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions are
#  met:
#
#      * Redistributions of source code must retain the above
#        copyright notice, this list of conditions and the following
#        disclaimer.
#
#      * Redistributions in binary form must reproduce the above
#        copyright notice, this list of conditions and the following
#        disclaimer in the documentation and/or other materials provided with
#        the distribution.
#
#      * Neither the name of Hypothetical Inc. nor the names of
#        any other contributors to this software may be used to endorse or
#        promote products derived from this software without specific prior
#        written permission.
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
#  IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
#  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
#  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
#  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
#  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
#  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
#  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
#  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
#  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
#  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
##########################################################################

# TODO: figure out how to get the secondary pool list from Deadline API


import Gaffer
import GafferDispatch
import GafferDeadline

Gaffer.Metadata.registerNode(

    GafferDeadline.DeadlineDispatcher,

    "description",
    """
    Dispatches tasks to Deadline.
    """

)

Gaffer.Metadata.registerNode(

    GafferDispatch.TaskNode,

    plugs={

        "dispatcher.deadline": [

            "description",
            """
            Settings that control how tasks are
            dispatched to Deadline.
            """,
            "layout:section", "Deadline",
            "plugValueWidget:type", "GafferUI.LayoutPlugValueWidget",

        ],

        "dispatcher.deadline.batchName": [
            "description",
            """
            The name of the Deadline batch for this job.
            """
        ],

        "dispatcher.deadline.comment": [
            "description",
            """
            A simple description of your job. This is optional and can be left blank.
            """,
        ],

        "dispatcher.deadline.department": [
            "description",
            """
            The department you belong to. This is optional and can be left blank.
            """,
        ],

        "dispatcher.deadline.pool": [
            "description",
            """
            The pool that the job will be submitted to.
            """,
            "plugValueWidget:type", "GafferDeadlineUI.DeadlineListPlugValueWidget",
            "deadlineListPlugValueWidget:type", "pools",
            "deadlineListPlugValueWidget:multiSelect", False,
            "userDefault", "none",
        ],

        "dispatcher.deadline.secondaryPool": [
            "description",
            """
            The secondary pool that the job will be submitted to.
            """,
            "plugValueWidget:type", "GafferDeadlineUI.DeadlineListPlugValueWidget",
            "deadlineListPlugValueWidget:type", "pools",
            "deadlineListPlugValueWidget:multiSelect", False,
            "userDefault", "none",
        ],

        "dispatcher.deadline.group": [
            "description",
            """
            The group that your job will be submitted to.
            """,
            "plugValueWidget:type", "GafferDeadlineUI.DeadlineListPlugValueWidget",
            "deadlineListPlugValueWidget:type", "groups",
            "deadlineListPlugValueWidget:multiSelect", False,
            "userDefault", "none",
        ],

        "dispatcher.deadline.priority": [
            "description",
            """
            A job can have a numeric priority ranging from 0 to 100, where 0 is the lowest
            priority and 100 is the highest.
            """,
        ],
        "dispatcher.deadline.taskTimeout": [
            "description",
            """
            The number of minutes a slave has to render a task for this job before it requeues it.
            Specify 0 for no timeout.
            """,
        ],
        "dispatcher.deadline.enableAutoTimeout": [
            "description",
            """
            If the Auto Task Timeout is properly configured in the repository options then
            enabling this will allow a task timeout to be automatically calculated based on render
            times for previous frames of the job.
            """,
        ],
        "dispatcher.deadline.concurrentTasks": [
            "description",
            """
            The number of tasks that can render concurrently on a single Slave. This is useful if
            the rendering application only
            uses one thread to render and your Slaves have multiple CPUs.
            """,
        ],
        "dispatcher.deadline.threads": [
            "description",
            """
            The number of threads Gaffer will use. Note that renderers and subprocesses launched
            by Gaffer may or may not respect this parameter.

            The actual value passed to Gaffer will be the lesser of this value and the number of
            CPU cores enabled by the Deadline Worker's CPU Affinity setting.

            If set to 0, this parameter is ignored.
            """
        ],
        "dispatcher.deadline.limitToSlaveLimit": [
            "description",
            """
            If you limit the tasks to a Slave's task limit, then by default, the Slave won't
            dequeue more tasks then it has CPUs.
            This task limit can be overridden for individual Slaves by an administrator.
            """,
        ],
        "dispatcher.deadline.machineLimit": [
            "description",
            """
            Use the Machine Limit to specify the maximum number of machines that can render your
            job at one time.
            Specify 0 for no limit.
            """,
        ],
        "dispatcher.deadline.machineList": [
            "description",
            """
            The whitelisted or blacklisted list of machines.
            """,
            "plugValueWidget:type", "GafferDeadlineUI.DeadlineListPlugValueWidget",
            "deadlineListPlugValueWidget:type", "slaves",
            "deadlineListPlugValueWidget:multiSelect", True
        ],
        "dispatcher.deadline.isBlackList": [
            "description",
            """
            You can force the job to render on specific machines by using a whitelist,
            or you can avoid specific machines by using a blacklist.
            """,
        ],
        "dispatcher.deadline.limits": [
            "description",
            """
            The Limits that your job requires.
            """,
            "plugValueWidget:type", "GafferDeadlineUI.DeadlineListPlugValueWidget",
            "deadlineListPlugValueWidget:type", "limits",
            "deadlineListPlugValueWidget:multiSelect", True
        ],
        "dispatcher.deadline.onJobComplete": [
            "description",
            """
            If desired, you can automatically archive or delete the job when it completes.
            """,
            "preset:Nothing", "Nothing",
            "preset:Archive", "Archive",
            "preset:Delete", "Delete",

            "userDefault", "Nothing",

            "plugValueWidget:type", "GafferUI.PresetsPlugValueWidget",
        ],
        "dispatcher.deadline.submitSuspended": [
            "description",
            """
            If enabled, the job will submit in the suspended state. This is useful if you
            don't want the job to start rendering right away. Just resume it from the Monitor
            when you want it to render.
            """,
        ],
        "dispatcher.deadline.dependencyMode": [
            "description",
            """
            Determine how downstream nodes that depend on this node will be handled. If set to
            auto, the dispatcher will attempt to determine the best mode, falling back to scripted
            dependency checking.
            """,
            "preset:Auto", "Auto",
            "preset:Full Job", "Job",
            "preset:Per Frame", "Frame",
            "preset:Scripted", "Script",
            "preset:Scripted", "None",

            "userDefault", "Auto",

            "plugValueWidget:type", "GafferUI.PresetsPlugValueWidget",
        ],
        "dispatcher.deadline.logLevel": [
            "description",
            """
            The value to use for the environment variable `IECORE_LOG_LEVEL`. Note that some
            renderers require a particular log level to updated render progress in the Gaffer
            Deadline plugin.
            """,
            "preset:Error", "ERROR",
            "preset:Warning", "WARNING",
            "preset:Info", "INFO",
            "preset:Debug", "DEBUG",
            "plugValueWidget:type", "GafferUI.PresetsPlugValueWidget",
        ],
        "dispatcher.deadline.outputs": [
            "description",
            """
            The outputs to pass to the Deadline job. Frame substitutions will not be made in
            order to allow Deadline to substitute frame numbers. All other substitutions are made.
            """
        ],
        "dispatcher.deadline.auxFiles": [
            "description",
            """
            A list of additional files to be included with the Deadline submission as auxiliary
            files. The submitter will upload them to the Deadline repository and Workers will
            download the files to their local job directory. An environment variable
            AUXFILEDIRECTORY is set by Deadline and can be referenced in Gaffer scripts using
            standard environment variable substitution such as ${AUXFILEDIRECTORY}/file.exr
            """,
            "plugValueWidget:type", "GafferUI.FileSystemPathVectorDataPlugValueWidget",
        ],
        "dispatcher.deadline.deadlineSettings": [
            "description",
            """
            A list of additional Deadline settings for the dispatched job. These variables are set
            after all other settings. Adding a variable here of "Name", for example, will override
            the default job name. A list of available settings can be found on the Manual
            Submission page of the Deadline documentation.
            """,
            "layout:section", "Deadline Settings",
        ],
        "dispatcher.deadline.environmentVariables": [
            "description",
            """
            A list of additional environment variables for Deadline to set before starting the job.
            """,
            "layout:section", "Environment Variables",
        ],
        "dispatcher.deadline.extraDeadlineSettings": [
            "description",
            """
            An additional set of Deadline settings for the job. Arbitrary numbers
            of settings may be specified within a single `IECore.CompoundObject`,
            where each key/value pair in the object defines a setting.
            This is convenient when using an expression to define the settings
            and the setting count might be dynamic.

            If the same setting is defined by both the settings and the
            extraSettings plugs, then the value from the extraSettings
            is taken.
            """
        ],
        "dispatcher.deadline.extraEnvironmentVariables": [
            "description",
            """
            An additional set of environment variables for the job. Arbitrary numbers
            of variables may be specified within a single `IECore.CompoundObject`,
            where each key/value pair in the object defines a variable.
            This is convenient when using an expression to define the variables
            and the setting count might be dynamic.

            If the same variable is defined by both the variables and the
            extraEnvironmentVariables plugs, then the value from the
            extraEnvironmentVariables is taken.
            """
        ],
    }

)
