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

import Gaffer
import GafferDispatch

from . import DeadlineTools
from .GafferDeadlineTask import GafferDeadlineTask
from .GafferDeadlineDependency import GafferDeadlineDependency


class GafferDeadlineJob(object):
    """ Manage simple aspects of a Deadline job for use with GafferDeadline.
    This is not meant to be comprehensive but only to provide the functionality
    to submit jobs and keep track of their job ids after submission. Hard coded
    for Gaffer plugin Deadline jobs for simplicity.
    """

    class DeadlineDependencyType(object):
        _None = 0
        JobToJob = 1
        FrameToFrame = 2
        Scripted = 3

    def __init__(
        self,
        gafferNode,
        jobProperties={},
        pluginProperties={},
        auxFiles=[],
        deadlineSettings={},
        environmentVariables={},
        jobContext=Gaffer.Context()
    ):
        self._dependencyType = None
        self._frameDependencyOffsetStart = 0
        self._frameDependencyOffsetEnd = 0

        self.setJobProperties(jobProperties)
        self.setPluginProperties(pluginProperties)
        self.setAuxFiles(auxFiles)
        self.setGafferNode(gafferNode)
        self.setContext(jobContext)

        self._deadlineSettings = deadlineSettings.copy()
        self._environmentVariables = environmentVariables.copy()
        self._jobId = None
        self._parentJobs = []
        self._tasks = []

    def __hash__(self):
        h = IECore.MurmurHash()
        for k, v in self.getJobProperties().items():
            h.append(k)
            h.append(v)

        for k, v in self.getPluginProperties().items():
            h.append(k)
            h.append(v)

        for f in self.getAuxFiles():
            h.append(f)

        for k, v in self._deadlineSettings.items():
            h.append(k)
            h.append(v)

        for k, v in self._environmentVariables.items():
            h.append(k)
            h.append(v)

        h.append(self._frameDependencyOffsetStart)
        h.append(self._frameDependencyOffsetEnd)

        # We don't need to hash the whole node with context because Gaffer takes care of
        # setting the context for the task. We can execute the same Deadline job with
        # different contexts.
        if self.getGafferNode():
            h.append(self.getGafferNode().getName())

        return hash(h)

    def setJobProperties(self, newProperties):
        """ The only parameter Deadline requires is Plugin and because we are
        focusing on Gaffer plugins, make sure that's always set.
        """
        assert(type(newProperties) == dict)
        self._jobProperties = newProperties
        self._jobProperties.update({"Plugin": "Gaffer"})

    def getJobProperties(self):
        return self._jobProperties

    def setDependencyType(self, depType):
        self._dependencyType = depType

    def getDependencyType(self):
        return self._dependencyType

    def setPluginProperties(self, newProperties):
        assert(type(newProperties) == dict)
        self._pluginProperties = newProperties.copy()

    def getPluginProperties(self):
        return self._pluginProperties

    def setAuxFiles(self, newAuxFiles):
        assert(type(newAuxFiles) == list or type(newAuxFiles) == str)
        newAuxFiles = newAuxFiles if type(newAuxFiles) == list else [newAuxFiles]
        self._auxFiles = newAuxFiles

    def getAuxFiles(self):
        return self._auxFiles

    def getJobID(self):
        return self._jobId

    def setGafferNode(self, newNode):
        if not issubclass(type(newNode), GafferDispatch.TaskNode) and newNode is not None:
            raise ValueError("Gaffer node must be a GafferDispatch.TaskNode or None")
        self._gafferNode = newNode

    def getGafferNode(self):
        return self._gafferNode

    def setContext(self, newContext):
        self._context = newContext

    def getContext(self):
        return self._context

    def addParentJob(self, parentJob):
        if type(parentJob) != GafferDeadlineJob:
            raise ValueError("Parent job must be a GafferDeadlineJob")
        if parentJob not in self.getParentJobs():
            self._parentJobs.append(parentJob)

    def getParentJobs(self):
        return self._parentJobs

    def getEffectiveParentJobs(self):
        jobs = []
        for j in self.getParentJobs():
            if not GafferDeadlineJob.isControlTask(j.getGafferNode()):
                jobs.append(j)
            else:
                jobs += j.getEffectiveParentJobs()

        return jobs

    def getParentJobByGafferNode(self, gafferNode):
        for job in self.getParentJobs():
            if job.getGafferNode() == gafferNode:
                return job

        return None

    def appendEnvironmentVariable(self, name, value):
        self._environmentVariables[name] = value

    def appendDeadlineSetting(self, name, value):
        self._deadlineSettings[name] = value

    # Separate batchFrames out so it can be unit tested. Gaffer does not allow creating
    # _TaskBatch objects
    def addBatch(self, newBatch, batchFrames):
        """ A batch corresponds to one or more Deadline Tasks
        Deadline Tasks must be sequential frames with only a start and end frame
        """
        assert(newBatch is None or type(newBatch) == GafferDispatch.Dispatcher._TaskBatch)
        # some TaskNodes like TaskList and TaskWedge submit with no frames because they are just
        # hierarchy placeholders they still need to be in for proper dependency handling
        if len(batchFrames) > 0:
            currentTask = GafferDeadlineTask(
                newBatch,
                len(self.getTasks()),
                startFrame=batchFrames[0],
                endFrame=batchFrames[0]
            )
            self._tasks.append(currentTask)
            for i in range(1, len(batchFrames)):
                if (batchFrames[i] - batchFrames[i-1]) > 1:
                    currentTask = GafferDeadlineTask(
                        newBatch,
                        len(self.getTasks()),
                        startFrame=batchFrames[i],
                        endFrame=batchFrames[i]
                    )
                    self._tasks.append(currentTask)
                else:
                    currentTask.setEndFrame(batchFrames[i])
        else:
            # Control nodes like TaskList have no frames but do need tasks created to pass
            # through dependencies
            self._tasks.append(GafferDeadlineTask(newBatch, 0))

    def getTasksForBatch(self, batch):
        taskList = [t for t in self.getTasks() if t.getGafferBatch() == batch]
        return taskList

    def getTasks(self):
        return self._tasks

    def __getParentBatches(self, batch):
        # Return the dependencies from a specific node, passing through the upstream nodes
        # if this is a control node.
        batches = []

        effectiveParentJobs = self.getEffectiveParentJobs()

        for b in batch.preTasks():
            if not GafferDeadlineJob.isControlTask(b.node()):
                job = None
                for j in effectiveParentJobs:
                    if j.getGafferNode() == b.node():
                        job = j
                if job is not None:
                    batches.append((job, b))
            else:
                batches += self.__getParentBatches(b)

        return batches

    def getDependencies(self):
        """ Link tasks to each other via their batch. Batches come from Gaffer and are what
        ultimately need to be linked. But there may be more than one task for a particular batch.

        If any of our parents are control tasks, we inherit their dependencies because they
        will not be submitted to Deadline.
        """

        effectiveParentJobs = []

        deps = {}
        for task in self.getTasks():
            for parentJob, parentBatch in self.__getParentBatches(task.getGafferBatch()):
                upstreamTasks = parentJob.getTasksForBatch(parentBatch)
                for dep in upstreamTasks:
                    deps[hash(task) + hash(dep) + hash(self)] = GafferDeadlineDependency(
                        parentJob,
                        task,
                        dep
                    )

        return deps

    @staticmethod
    def isControlTask(node):
        return type(node) in [
            GafferDispatch.FrameMask,
            GafferDispatch.TaskList,
            GafferDispatch.TaskSwitch,
            GafferDispatch.Wedge,
            GafferDispatch.TaskContextVariables,  # this node is deprecated and will be removed
        ]

    def submitJob(self, jobFilePath=None, pluginFilePath=None):
        """ Submit the job to Deadline.
        Returns a tuple of (submittedJobId, deadlineStatusOutput). submittedJobId
        will be None if submission failed. deadlineStatusOutput can be used to help figure out
        why it failed.

        Check to make sure that all auxiliary files exist, otherwise submission will fail.
        Job and plugin information are stored in temporary files that are deleted after submission.
        Windows has a problem with allowing Python to hide the temp file from the OS,
        so the delete=False argument must be passed.

        Job and plugin files are just serializations of their respective dictionaries in the
        form of key=value separated by newlines.

        If the jobFile or pluginFile are not supplied, create temp files for them.
        Only remove temp files, not supplied files.
        """
        for auxFile in self._auxFiles:
            if not os.path.isfile(auxFile):
                raise IOError("{} does not exist".format(auxFile))

        if jobFilePath is None:
            jobFile = tempfile.NamedTemporaryFile(mode="w", suffix=".info", delete=False)
        else:
            jobFile = open(jobFilePath, mode="w")

        self._jobProperties.update(self._deadlineSettings)
        jobLines = [
            "{}={}".format(k, self._jobProperties[k]) for k in self._jobProperties.keys()
        ]
        environmentVariableCounter = 0
        for v in self._environmentVariables.keys():
            jobLines.append(
                "EnvironmentKeyValue{}={}={}".format(
                    environmentVariableCounter,
                    v,
                    self._environmentVariables[v]
                )
            )
            environmentVariableCounter += 1
        # Default to IECORE_LOG_LEVEL=INFO
        if "IECORE_LOG_LEVEL" not in self._environmentVariables:
            jobLines.append(
                "EnvironmentKeyValue{}=IECORE_LOG_LEVEL=INFO".format(
                    environmentVariableCounter
                )
            )

        jobFile.write("\n".join(jobLines))
        jobFile.close()

        if pluginFilePath is None:
            pluginFile = tempfile.NamedTemporaryFile(mode="w", suffix=".info", delete=False)
        else:
            pluginFile = open(pluginFilePath, mode="w")
        pluginLines = [
            "{}={}".format(
                k,
                self._pluginProperties[k]
            ) for k in self._pluginProperties.keys()
        ]
        pluginFile.write("\n".join(pluginLines))
        pluginFile.close()

        result = DeadlineTools.submitJob(jobFile.name, pluginFile.name, self._auxFiles)

        IECore.Log.debug("Submission results:", result)

        if result[0] is None:
            raise RuntimeError("Deadline submission failed: \n{}".format(result[1]))
        self._jobId = result[0]

        if jobFile is None:
            os.remove(jobFile)
        if pluginFile is None:
            os.remove(pluginFile)

        return (self._jobId, result[1])
