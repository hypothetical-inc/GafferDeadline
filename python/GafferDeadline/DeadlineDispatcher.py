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

import IECore

import Gaffer
import GafferDispatch

import GafferDeadline


class DeadlineDispatcher(GafferDispatch.Dispatcher):

    def __init__(self, name="DeadlineDispatcher"):
        GafferDispatch.Dispatcher.__init__(self, name)
        self._deadlineJobs = []

    # Emitted prior to submitting the Deadline job, to allow
    # custom modifications to be applied.
    #
    # Slots should have the signature `slot( dispatcher, job )`,
    # where dispatcher is the DeadlineDispatcher and job will
    # be the instance of GafferDeadlineJob that is about
    # to be spooled.
    @classmethod
    def preSpoolSignal(cls):
        return cls.__preSpoolSignal

    __preSpoolSignal = Gaffer.Signal2()

    def _doDispatch(self, rootBatch):
        '''
        _doDispatch is called by Gaffer, the others (prefixed with __) are just helpers for
        Deadline

        Note that Gaffer and Deadline use some terms differently
        Gaffer Batch =~ Deadline Task, which could be multiple frames in a single task. Depending
                        on batch layout multiple Deadline Tasks may be needed to fullfill a single
                        Gaffer Batch. For example, a Deadline Task can only handle sequential
                        frames.
        Gaffer TaskNode =~ Deadline Job. A Gaffer Task can have multiple Deadline Jobs to complete
                            it depending on batch and context layout. A DeadlineJob is defined by
                            the combination of Gaffer TaskNode and Context.
        Gaffer Job = set of Deadline Jobs (could be considered a Deadline Batch)

        Use DeadlineJob, DeadlineTask, etc. to denote Deadline terminology and plain Batch, Job,
        etc. to denote Gaffer terminology.

        Batches can have dependencies completely independent of frame numbers. First
        walk through the batch tree to build up a set of GafferDeadlineJob objects with
        GafferDeadlineTask objects corresponding to the batches.

        When all tasks are created, go back through the tree to setup dependencies between tasks.
        Task dependencies may be different from Batch dependencies because batches may have been
        split to accommodate Deadline's sequential frame task requirement.

        With dependencies set, start at the leaf nodes of the task tree (no upstream DeadlineJobs)
        and submit those first. That way the Deadline Job ID can be stored and used by dependent
        jobs to set their dependencies correctly.

        To be compatible with Deadline's ExtraInfoKeyValue system, dependencies are reformatted at
        submission as task:jobDependencyId=taskDependencyNumber
        '''
        self._deadlineJobs = []
        IECore.Log.info("Beginning Deadline submission")
        dispatchData = {}
        dispatchData["scriptNode"] = rootBatch.preTasks()[0].node().scriptNode()
        dispatchData["scriptFile"] = os.path.join(
            self.jobDirectory(),
            os.path.basename(
                dispatchData["scriptNode"]["fileName"].getValue()
            ) or "untitled.gfr"
        )
        dispatchData["scriptFile"] = dispatchData["scriptFile"].replace("\\", os.sep).replace(
            "/",
            os.sep
        )

        dispatchData["scriptNode"].serialiseToFile(dispatchData["scriptFile"])

        with Gaffer.Context.current() as c:
            dispatchData["dispatchJobName"] = self["jobName"].getValue()

        rootDeadlineJob = GafferDeadline.GafferDeadlineJob(rootBatch.node())
        rootDeadlineJob.setAuxFiles([dispatchData["scriptFile"]])
        self.__addGafferDeadlineJob(rootDeadlineJob)
        rootJobs = []
        for upstreamBatch in rootBatch.preTasks():
            rootJob = self.__buildDeadlineJobWalk(upstreamBatch, dispatchData)
            if rootJob is not None:
                rootJobs.append(rootJob)

        rootJobs = list(set(rootJobs))

        for rootJob in rootJobs:
            self.__submitDeadlineJob(rootJob, dispatchData)

    def __buildDeadlineJobWalk(self, batch, dispatchData):
        IECore.msg(
            IECore.Msg.Level.Debug,
            "DeadlineDispatcher",
            "Build DeadlineJob from batch : plug = {}, frames = {}".format(
                batch.plug().getName(),
                batch.frames()
            )
        )
        if (
            GafferDeadline.GafferDeadlineJob.isControlTask(batch.node()) and
            batch.node()["dispatcher"]["batchSize"].getValue() > 1
        ):
            IECore.msg(
                IECore.Msg.Level.Warning,
                "DeadlineDispatcher",
                "No-Op node {} has a batch size greater than 1 which will be ignored.".format(
                    batch.node().getName()
                )
            )

        if batch.blindData().get("deadlineDispatcher:visited"):
            return self.__getGafferDeadlineJob(batch.node(), batch.context())

        deadlineJob = self.__getGafferDeadlineJob(batch.node(), batch.context())
        if not deadlineJob:
            deadlineJob = GafferDeadline.GafferDeadlineJob(batch.node())
            deadlineJob.setContext(batch.context())
            deadlineJob.setAuxFiles([dispatchData["scriptFile"]])
            self.__addGafferDeadlineJob(deadlineJob)

        deadlineJob.addBatch(batch, batch.frames())
        for upstreamBatch in batch.preTasks():
            parentDeadlineJob = self.__buildDeadlineJobWalk(upstreamBatch, dispatchData)
            if parentDeadlineJob is not None:
                deadlineJob.addParentJob(parentDeadlineJob)

        batch.blindData()["deadlineDispatcher:visited"] = IECore.BoolData(True)

        return deadlineJob

    def __getGafferDeadlineJob(self, node, context):
        for j in self._deadlineJobs:
            if j.getGafferNode() == node and j.getContext() == context:
                return j

    def __addGafferDeadlineJob(self, newDeadlineJob):
        self._deadlineJobs.append(newDeadlineJob)
        self._deadlineJobs = list(set(self._deadlineJobs))

    def __submitDeadlineJob(self, deadlineJob, dispatchData):
        # submit jobs depth first so parent job IDs will be populated
        for parentJob in deadlineJob.getParentJobs():
            self.__submitDeadlineJob(parentJob, dispatchData)

        gafferNode = deadlineJob.getGafferNode()

        # Don't submit command tasks, they pollute the Deadline Monitor and cause
        # potentially lengthy delays in dequeuing tasks that do nothing.
        if GafferDeadline.GafferDeadlineJob.isControlTask(deadlineJob.getGafferNode()):
            return None

        # this job is already submitted if it has an ID
        if deadlineJob.getJobID() is not None:
            return deadlineJob.getJobID()

        self.preSpoolSignal()(self, deadlineJob)

        deadlinePlug = gafferNode["dispatcher"].getChild("deadline")

        if deadlinePlug is not None:
            initialStatus = (
                "Suspended" if deadlinePlug["submitSuspended"].getValue() else "Active"
            )
            machineListType = (
                "Blacklist" if deadlinePlug["isBlackList"].getValue() else "Whitelist"
            )

            # to prevent Deadline from splitting up our tasks (since we've already done that based
            # on batches), set the chunk size to the largest frame range
            chunkSize = (
                deadlineJob.getTasks()[0].getEndFrame() -
                deadlineJob.getTasks()[0].getStartFrame() + 1
            )
            frameString = ""
            for t in deadlineJob.getTasks():
                chunkSize = max(t.getEndFrame() - t.getStartFrame() + 1, chunkSize)
                if t.getStartFrame() == t.getEndFrame():
                    frameString += ",{}".format(t.getStartFrame())
                else:
                    frameString += ",{}-{}".format(t.getStartFrame(), t.getEndFrame())

            with Gaffer.Context(deadlineJob.getContext()) as c:
                jobInfo = {
                    "Name": (
                        "{}{}{}".format(
                            dispatchData["dispatchJobName"],
                            "." if dispatchData["dispatchJobName"] else "",
                            gafferNode.relativeName(dispatchData["scriptNode"]),
                        )
                    ),
                    "Frames": frameString,
                    "ChunkSize": chunkSize,
                    "Plugin": "Gaffer" if not isinstance(
                        gafferNode,
                        GafferDeadline.DeadlineTask
                    ) else gafferNode["plugin"].getValue(),
                    "BatchName": deadlinePlug["batchName"].getValue(),
                    "Comment": deadlinePlug["comment"].getValue(),
                    "Department": deadlinePlug["department"].getValue(),
                    "Pool": deadlinePlug["pool"].getValue(),
                    "SecondaryPool": deadlinePlug["secondaryPool"].getValue(),
                    "Group": deadlinePlug["group"].getValue(),
                    "Priority": deadlinePlug["priority"].getValue(),
                    "TaskTimeoutMinutes": int(deadlinePlug["taskTimeout"].getValue()),
                    "EnableAutoTimeout": deadlinePlug["enableAutoTimeout"].getValue(),
                    "ConcurrentTasks": deadlinePlug["concurrentTasks"].getValue(),
                    "MachineLimit": deadlinePlug["machineLimit"].getValue(),
                    machineListType: deadlinePlug["machineList"].getValue(),
                    "LimitGroups": deadlinePlug["limits"].getValue(),
                    "OnJobComplete": deadlinePlug["onJobComplete"].getValue(),
                    "InitialStatus": initialStatus,
                }

                auxFiles = deadlineJob.getAuxFiles()   # this will already have substitutions included
                auxFiles += [f for f in deadlinePlug["auxFiles"].getValue()]
                deadlineJob.setAuxFiles(auxFiles)

                for output in deadlinePlug["outputs"].getValue():
                    deadlineJob.addOutput(output, c)

                environmentVariables = IECore.CompoundData()

                deadlinePlug["environmentVariables"].fillCompoundData(environmentVariables)
                extraEnvironmentVariables = deadlinePlug["extraEnvironmentVariables"].getValue()
                for name, value in extraEnvironmentVariables.items():
                    environmentVariables[name] = value
                for name, value in environmentVariables.items():
                    deadlineJob.appendEnvironmentVariable(name, str(value))

                deadlineSettings = IECore.CompoundData()
                deadlinePlug["deadlineSettings"].fillCompoundData(deadlineSettings)
                extraDeadlineSettings = deadlinePlug["extraDeadlineSettings"].getValue()
                for name, value in extraDeadlineSettings.items():
                    deadlineSettings[name] = value
                for name, value in deadlineSettings.items():
                    deadlineJob.appendDeadlineSetting(name, str(value))

            """ Dependencies are stored with a reference to the Deadline job since job IDs weren't
            assigned when the task tree was walked. Now that parent jobs have been submitted and
            have IDs, we can substitute that in for the dependency script to pick up.

            We also want to dependencies to be as native to Deadline as possible, resorting to the
            dependency script only in cases where it is needed (Deadline's dependency script
            triggering seems to be slower than native task dependencies)

            There are three possible dependency types allowed by Deadline:
            1) Job-Job:     All of the tasks for job A wait for all of the tasks for job B to
                            finish before job A runs. This is relatively rare when coming from
                            Deadline and mostly is used by nodes upstream from a FrameMask node.
                            In that case releasing tasks per-frame would trigger downstream jobs
                            sooner than they should.
            2) Frame-Frame: This is somewhat misleadingly named because Deadline only checks for
                            frame dependency release after each task completes, so this is very
                            similar to task-task dependencies. Deadline can only handle a start
                            and end frame offset when comparing to the parent job so the task
                            offsets must match across all parent jobs to enable this mode.
            3) Task-Task:   A task for job A waits for a task for job B to finish before the task
                            for job A runs. If the dependency start and end frame offsets don't
                            match, this has to be handled by a dependency script.
            """
            dependencies = deadlineJob.getDependencies().values()

            if len(dependencies) > 0 and deadlinePlug["dependencyMode"].getValue() != "None":
                jobDependent = False
                frameDependent = False
                simpleFrameOffset = True
                if deadlinePlug["dependencyMode"].getValue() == "Job":
                    jobDependent = True
                elif deadlinePlug["dependencyMode"].getValue() == "Frame":
                    frameDependent = True
                elif deadlinePlug["dependencyMode"].getValue() == "Auto":
                    jobDependent = False
                    dependencyJobs = [i.getDeadlineJob() for i in dependencies]
                    dependencyJobs = list(set(dependencyJobs))

                    frameDependent = True
                    simpleFrameOffset = True
                    if (len(dependencies) > 0):
                        deadlineJob._frameDependencyOffsetStart = (
                            list(dependencies)[0].getUpstreamDeadlineTask().getStartFrame() -
                            list(dependencies)[0].getDeadlineTask().getStartFrame()
                        )
                        deadlineJob._frameDependencyOffsetEnd = (
                            list(dependencies)[0].getUpstreamDeadlineTask().getEndFrame() -
                            list(dependencies)[0].getDeadlineTask().getEndFrame()
                        )

                        for d in dependencies:
                            newFrameOffsetStart = (
                                d.getUpstreamDeadlineTask().getStartFrame() -
                                d.getDeadlineTask().getStartFrame()
                            )
                            newFrameOffsetEnd = (
                                d.getUpstreamDeadlineTask().getEndFrame() -
                                d.getDeadlineTask().getEndFrame()
                            )

                            if (
                                newFrameOffsetStart != deadlineJob._frameDependencyOffsetStart or
                                newFrameOffsetEnd != deadlineJob._frameDependencyOffsetEnd
                            ):
                                simpleFrameOffset = False

                        # If we can't just shift the frame start and end, we might still be able to
                        # use frame dependency with tasks of different frame lengths
                        if not simpleFrameOffset:
                            for j in dependencyJobs:
                                dependencyTasks = [
                                    t.getUpstreamDeadlineTask() for t in dependencies if (
                                        t.getDeadlineJob() == j
                                    )
                                ]
                                dependencyFrames = []
                                for t in dependencyTasks:
                                    dependencyFrames += range(
                                        t.getStartFrame(),
                                        t.getEndFrame() + 1
                                    )
                                currentFrames = []
                                for t in deadlineJob.getTasks():
                                    currentFrames += range(
                                        t.getStartFrame(),
                                        t.getEndFrame() + 1
                                    )
                                for f in currentFrames:
                                    if f not in dependencyFrames:
                                        frameDependent = False

                    else:
                        frameDependent = False

                if jobDependent or frameDependent:
                    jobInfo.update(
                        {
                            "JobDependencies": ",".join(
                                list(set([
                                    d.getDeadlineJob().getJobID() for d in dependencies
                                ]))
                            ),
                            "ResumeOnDeletedDependencies": True,
                        }
                    )
                    if simpleFrameOffset:
                        jobInfo.update(
                            {
                                "FrameDependencyOffsetStart": (
                                    deadlineJob._frameDependencyOffsetStart
                                ),
                                "FrameDependencyOffsetEnd": deadlineJob._frameDependencyOffsetEnd,
                            }
                        )
                    if frameDependent:
                        jobInfo.update({"IsFrameDependent": True})
                        deadlineJob.setDependencyType(
                            GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.FrameToFrame
                        )
                    else:
                        deadlineJob.setDependencyType(
                            GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.JobToJob
                        )
                else:
                    jobInfo.update(
                        {
                            "ScriptDependencies": os.environ["DEADLINE_DEPENDENCY_SCRIPT_PATH"],
                            "IsFrameDependent": True,
                        }
                    )
                    i = 0
                    for d in dependencies:
                        jobInfo["ExtraInfoKeyValue{}".format(i)] = "{}:{}={}".format(
                            int(d.getDeadlineTask().getTaskNumber()),
                            d.getDeadlineJob().getJobID(),
                            d.getUpstreamDeadlineTask().getTaskNumber()
                        )

                        i += 1

                    deadlineJob.setDependencyType(
                        GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.Scripted
                    )
            else:
                deadlineJob.setDependencyType(
                    GafferDeadline.GafferDeadlineJob.DeadlineDependencyType._None
                )

            pluginInfo = {}
            if not isinstance(gafferNode, GafferDeadline.DeadlineTask):
                pluginInfo = {
                    "Script": os.path.split(dispatchData["scriptFile"])[-1],
                    "Version": Gaffer.About.versionString(),
                    "IgnoreScriptLoadErrors": False,
                    "Nodes": gafferNode.relativeName(dispatchData["scriptNode"]),
                    "Frames": "<STARTFRAME>-<ENDFRAME>",
                    "Threads": deadlinePlug["threads"].getValue(),
                }
            else:
                data = IECore.CompoundData()
                gafferNode["parameters"].fillCompoundData(data)
                pluginInfo = dict(data)

            scriptContext = dispatchData["scriptNode"].context()
            contextArgs = []
            for entry in [
                k for k in deadlineJob.getContext().keys() if (
                    k != "frame" and
                    not k.startswith("ui:")
                )
            ]:
                if (
                    entry not in scriptContext.keys() or
                    deadlineJob.getContext()[entry] != scriptContext[entry]
                ):
                    contextArgs.extend(
                        [
                            "\"-{}\"".format(entry),
                            "\"{}\"".format(repr(deadlineJob.getContext()[entry]))
                        ]
                    )
            if contextArgs and not isinstance(gafferNode, GafferDeadline.DeadlineTask):
                pluginInfo["Context"] = " ".join(contextArgs)

            deadlineJob.setJobProperties(jobInfo)
            deadlineJob.setPluginProperties(pluginInfo)

            deadlineJob.setLogLevel(deadlinePlug["logLevel"].getValue())

            jobId, output = deadlineJob.submitJob(self.jobDirectory())
            if jobId is None:
                IECore.Log.error(jobInfo["Name"], "failed to submit to Deadline.", output)
            else:
                IECore.Log.info(jobInfo["Name"], "submission succeeded.", output)

            return deadlineJob.getJobID()
        else:
            IECore.Log.error("GafferDeadline", "Failed to acquire Deadline plug")
            return None

    @staticmethod
    def _setupPlugs(parentPlug):

        if "deadline" in parentPlug:
            return

        parentPlug["deadline"] = Gaffer.Plug()
        parentPlug["deadline"]["batchName"] = Gaffer.StringPlug(defaultValue="${script:name}")
        parentPlug["deadline"]["comment"] = Gaffer.StringPlug()
        parentPlug["deadline"]["department"] = Gaffer.StringPlug()
        parentPlug["deadline"]["pool"] = Gaffer.StringPlug()
        parentPlug["deadline"]["secondaryPool"] = Gaffer.StringPlug()
        parentPlug["deadline"]["group"] = Gaffer.StringPlug()
        parentPlug["deadline"]["priority"] = Gaffer.IntPlug(
            defaultValue=50,
            minValue=0,
            maxValue=100
        )
        parentPlug["deadline"]["taskTimeout"] = Gaffer.IntPlug(defaultValue=0, minValue=0)
        parentPlug["deadline"]["enableAutoTimeout"] = Gaffer.BoolPlug(defaultValue=False)
        parentPlug["deadline"]["concurrentTasks"] = Gaffer.IntPlug(
            defaultValue=1,
            minValue=1,
            maxValue=16
        )
        parentPlug["deadline"]["threads"] = Gaffer.IntPlug(defaultValue=0, minValue=0)
        parentPlug["deadline"]["machineLimit"] = Gaffer.IntPlug(defaultValue=0, minValue=0)
        parentPlug["deadline"]["machineList"] = Gaffer.StringPlug()
        parentPlug["deadline"]["isBlackList"] = Gaffer.BoolPlug(defaultValue=False)
        parentPlug["deadline"]["limits"] = Gaffer.StringPlug()
        parentPlug["deadline"]["onJobComplete"] = Gaffer.StringPlug(defaultValue="Nothing")
        parentPlug["deadline"]["submitSuspended"] = Gaffer.BoolPlug(defaultValue=False)
        parentPlug["deadline"]["dependencyMode"] = Gaffer.StringPlug(defaultValue="Auto")
        parentPlug["deadline"]["logLevel"] = Gaffer.StringPlug(defaultValue="INFO")
        parentPlug["deadline"]["outputs"] = Gaffer.StringVectorDataPlug(
            defaultValue=IECore.StringVectorData()
        )
        parentPlug["deadline"]["auxFiles"] = Gaffer.StringVectorDataPlug(
            defaultValue=IECore.StringVectorData()
        )
        parentPlug["deadline"]["deadlineSettings"] = Gaffer.CompoundDataPlug()
        parentPlug["deadline"]["environmentVariables"] = Gaffer.CompoundDataPlug()
        parentPlug["deadline"]["extraDeadlineSettings"] = Gaffer.CompoundObjectPlug()
        parentPlug["deadline"]["extraEnvironmentVariables"] = Gaffer.CompoundObjectPlug()


IECore.registerRunTimeTyped(DeadlineDispatcher, typeName="GafferDeadline::DeadlineDispatcher")

GafferDispatch.Dispatcher.registerDispatcher(
    "Deadline",
    DeadlineDispatcher,
    DeadlineDispatcher._setupPlugs
)
