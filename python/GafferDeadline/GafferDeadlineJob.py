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

import os
import tempfile

import IECore

import GafferDispatch
import GafferDeadline
import DeadlineTools


class GafferDeadlineJob(object):
    """ Manage simple aspects of a Deadline job for use with GafferDeadline.
    This is not meant to be comprehensive but only to provide the functionality
    to submit jobs and keep track of their job ids after submission. Hard coded
    for Gaffer plugin Deadline jobs for simplicity.
    """

    class DeadlineDependencyType(object):
        none = 0
        job_to_job = 1
        frame_to_frame = 2
        scripted = 3

    def __init__(self, job_properties={}, plugin_properties={}, aux_files=[], deadlineSettings={}, environmentVariables={}, gaffer_node=None, job_context={}, chunk_size=1):
        self._dependency_type = None
        self._frame_dependency_offset_start = 0
        self._frame_dependency_offset_end = 0

        self.setJobProperties(job_properties)
        self.setPluginProperties(plugin_properties)
        self.setAuxFiles(aux_files)
        self.setGafferNode(gaffer_node)
        self.setContext(job_context)
        
        self._deadlineSettings = deadlineSettings.copy()
        self._environmentVariables = environmentVariables.copy()
        self._job_id = None
        self._parent_jobs = []
        self._tasks = []
        # dependencies are of the dictionary form {"dependency_job": <GafferDeadlineJob>, "dependent_task": <GafferDeadlineTask for this job>, "dependency_task": <GafferDeadlineTask for parent job>)
        self._dependencies = []

    def setJobProperties(self, new_properties):
        """ The only parameter Deadline requires is Plugin and because we are
        focusing on Gaffer plugins, make sure that's always set.
        """
        assert(type(new_properties) == dict)
        self._job_properties = new_properties
        self._job_properties.update({"Plugin": "Gaffer"})

    def getJobProperties(self):
        return self._job_properties

    def setDependencyType(self, dep_type):
        self._dependency_type = dep_type

    def getDependencyType(self):
        return self._dependency_type

    def setPluginProperties(self, new_properties):
        assert(type(new_properties) == dict)
        self._plugin_properties = new_properties.copy()

    def getPluginProperties(self):
        return self._plugin_properties

    def setAuxFiles(self, new_aux_files):
        assert(type(new_aux_files) == list or type(new_aux_files) == str)
        new_aux_files = new_aux_files if type(new_aux_files) == list else [new_aux_files]
        self._aux_files = new_aux_files

    def getAuxFiles(self):
        return self._aux_files

    def getJobID(self):
        return self._job_id

    def setGafferNode(self, new_node):
        if not issubclass(type(new_node), GafferDispatch.TaskNode) and new_node is not None:
            raise (ValueError, "Gaffer node must be a GafferDispatch.TaskNode or None")
        self._gaffer_node = new_node

    def getGafferNode(self):
        return self._gaffer_node

    def setContext(self, new_context):
        self._context = new_context

    def getContext(self):
        return self._context

    def addParentJob(self, parent_job):
        if type(parent_job) != GafferDeadlineJob:
            raise (ValueError, "Parent job must be a GafferDeadlineJob")
        if parent_job not in self._parent_jobs:
            self._parent_jobs.append(parent_job)

    def getParentJobs(self):
        return self._parent_jobs

    def getParentJobByGafferNode(self, gaffer_node):
        for job in self._parent_jobs:
            if job.getGafferNode() == gaffer_node:
                return job

        return None

    def appendEnvironmentVariable(self, name, value):
        self._environmentVariables[name] = value

    def appendDeadlineSetting(self, name, value):
        self._deadlineSettings[name] = value

    # Separate batch_frames out so it can be unit tested. Gaffer does not allow creating _TaskBatch objects
    def addBatch(self, new_batch, batch_frames):
        """ A batch corresponds to one or more Deadline Tasks
        Deadline Tasks must be sequential frames with only a start and end frame
        """
        assert(new_batch is None or type(new_batch) == GafferDispatch.Dispatcher._TaskBatch)
        # some TaskNodes like TaskList and TaskWedge submit with no frames because they are just hierarchy placeholders
        # they still need to be in for proper dependency handling
        if len(batch_frames) > 0:
            current_task = GafferDeadline.GafferDeadlineTask(new_batch, len(self.getTasks()), start_frame=batch_frames[0], end_frame=batch_frames[0])
            self._tasks.append(current_task)
            for i in range(1, len(batch_frames)):
                if (batch_frames[i] - batch_frames[i-1]) > 1:
                    current_task = GafferDeadline.GafferDeadlineTask(new_batch, len(self.getTasks()), start_frame=batch_frames[i], end_frame=batch_frames[i])
                    self._tasks.append(current_task)
                else:
                    current_task.setEndFrame(batch_frames[i])
        else:
            # Control nodes like TaskList have no frames but do need tasks created to pass
            # through dependencies
            self._tasks.append(GafferDeadline.GafferDeadlineTask(new_batch, len(self.getTasks())))

    def getTasksForBatch(self, batch):
        task_list = [t for t in self.getTasks() if t.getGafferBatch() == batch]
        return task_list

    def getTasks(self):
        return self._tasks

    def buildTaskDependencies(self):
        """ Link tasks to each other via their batch. Batches come from Gaffer and are what ultimately
        need to be linked. But there may be more than one task for a particular batch.
        """
        for task in self.getTasks():
            for job in self._parent_jobs:
                for parent_batch in task.getGafferBatch().preTasks():
                    dependency_tasks = job.getTasksForBatch(parent_batch)
                    for dep in dependency_tasks:
                        # Control tasks should have their frames set to the upstream frame range
                        # effectively making them frame-frame dependent
                        if task.getStartFrame() is None or task.getEndFrame() is None:
                            task.setFrameRange(dep.getStartFrame(), dep.getEndFrame())
                        dep_dict = {
                            "dependency_job": job,
                            "dependent_task": task,
                            "dependency_task": dep
                        }
                        self._dependencies.append(dep_dict)

    def removeOrphanTasks(self):
        self._tasks = [t for t in self.getTasks() if t.getStartFrame() is not None or t.getEndFrame() is not None or len(t.getGafferBatch().preTasks()) > 0]
    
    def getDependencies(self):
        return self._dependencies

    def submitJob(self, job_file_path=None, plugin_file_path=None):
        """ Submit the job to Deadline.
        Returns a tuple of (submitted_job_id, Deadline_status_output). submitted_job_id
        will be None if submission failed. Deadline_status_output can be used to help figure out
        why it failed.

        Check to make sure that all auxiliary files exist, otherwise submission will fail
        Job and plugin information are stored in temporary files that are deleted after submission.
        Windows has a problem with allowing Python to hide the temp file from the OS,
        so the delete=False argument must be passed.

        Job and plugin files are just serializations of their respective dictionaries in the
        form of key=value separated by newlines.

        If the job_file or plugin_file are not supplied, create temp files for them.
        Only remove temp files, not supplied files.
        """
        for aux_file in self._aux_files:
            if not os.path.isfile(aux_file):
                raise IOError("{} does not exist".format(aux_file))

        if job_file_path is None:
            job_file = tempfile.NamedTemporaryFile(mode="w", suffix=".info", delete=False)
        else:
            job_file = open(job_file_path, mode="w")

        self._job_properties.update(self._deadlineSettings)
        job_lines = ["{}={}".format(k, self._job_properties[k]) for k in self._job_properties.keys()]
        environmentVariableCounter = 0
        for v in self._environmentVariables.keys():
            job_lines.append("EnvironmentKeyValue{}={}={}".format(environmentVariableCounter, v, self._environmentVariables[v]))
            environmentVariableCounter += 1
        # Default to IECORE_LOG_LEVEL=INFO
        if "IECORE_LOG_LEVEL" not in self._environmentVariables:
            job_lines.append("EnvironmentKeyValue{}=IECORE_LOG_LEVEL=INFO".format(environmentVariableCounter))

        job_file.write("\n".join(job_lines))
        job_file.close()

        if plugin_file_path is None:
            plugin_file = tempfile.NamedTemporaryFile(mode="w", suffix=".info", delete=False)
        else:
            plugin_file = open(plugin_file_path, mode="w")
        plugin_lines = ["{}={}".format(k, self._plugin_properties[k]) for k in self._plugin_properties.keys()]
        plugin_file.write("\n".join(plugin_lines))
        plugin_file.close()

        result = DeadlineTools.submitJob(job_file.name, plugin_file.name, self._aux_files)

        IECore.Log.debug("Submission results:", result)

        if result[0] is None:
            raise(RuntimeError, "Deadline submission failed: \n{}".format(result[1]))
        self._job_id = result[0]

        if job_file is None:
            os.remove(job_file)
        if plugin_file is None:
            os.remove(plugin_file)

        return (self._job_id, result[1])
