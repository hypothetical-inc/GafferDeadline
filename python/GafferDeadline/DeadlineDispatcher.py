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
        self._deadline_jobs = []

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

    def _doDispatch(self, root_batch):
        '''
        _doDispatch is called by Gaffer, the others (prefixed with __) are just helpers for Deadline

        Note that Gaffer and Deadline use some terms differently
        Gaffer Batch =~ Deadline Task, which could be multiple frames in a single task. Depending on batch layout
                        multiple Deadline Tasks may be needed to fullfill a single Gaffer Batch. For example, a Deadline
                        Task can only handle sequential frames.
        Gaffer TaskNode =~ Deadline Job. A Gaffer Task can have multiple Deadline Jobs to complete it depending on batch and context layout.
                            A DeadlineJob is defined by the combination of Gaffer TaskNode and Context.
        Gaffer Job = set of Deadline Jobs (could be considered a Deadline Batch)
        Use DeadlineJob, DeadlineTask, etc. to denote Deadline terminology and plain Batch, Job, etc. to denote Gaffer terminology.

        Batches can have dependencies completely independent of frame numbers. First
        walk through the batch tree to build up a set of GafferDeadlineJob objects with GafferDeadlineTask objects corresponding
        to the batches.

        When all tasks are created, go back through the tree to setup dependencies between tasks. Task dependencies may be different
        from Batch dependencies because batches may have been split to accommodate Deadline's sequential frame task requirement.

        With dependencies set, start at the leaf nodes of the task tree (no upstream DeadlineJobs) and submit those first. That way
        the Deadline Job ID can be stored and used by dependent jobs to set their dependencies correctly.

        To be compatible with Deadline's ExtraInfoKeyValue system, dependencies are reformatted at submission as
        task:job_dependency_id=task_dependency_number
        '''
        IECore.Log.info("Beginning Deadline submission")
        dispatch_data = {}
        dispatch_data["scriptNode"] = root_batch.preTasks()[0].node().scriptNode()
        dispatch_data["scriptFile"] = os.path.join(self.jobDirectory(), os.path.basename(dispatch_data["scriptNode"]["fileName"].getValue()) or "untitled.gfr")
        dispatch_data["scriptFile"] = dispatch_data["scriptFile"].replace("\\", os.sep).replace("/", os.sep)

        dispatch_data["scriptNode"].serialiseToFile(dispatch_data["scriptFile"])

        context = Gaffer.Context.current()
        dispatch_data["deadlineBatch"] = context.substitute(self["jobName"].getValue()) or "untitled"

        root_deadline_job = GafferDeadline.GafferDeadlineJob()
        root_deadline_job.setGafferNode(root_batch.node())
        root_deadline_job.setAuxFiles([dispatch_data["scriptFile"]])
        self.__addGafferDeadlineJob(root_deadline_job)
        root_jobs = []
        for upstream_batch in root_batch.preTasks():
            root_job = self.__buildDeadlineJobWalk(upstream_batch, dispatch_data)
            if root_job is not None:
                root_jobs.append(root_job)

        root_jobs = list(set(root_jobs))

        for root_job in root_jobs:
            self.__buildDeadlineDependencyWalk(root_job)
            # Control jobs with nothing to control should be removed after the dependencies are set up.
            # This mostly applies to FrameMask nodes where downstream nodes need to see tasks on the FrameMask
            # to trigger Job-Job dependency mode, but those tasks should not be submitted to Deadline.
            self.__removeOrphanTasksWalk(root_job)
            self.__submitDeadlineJob(root_job, dispatch_data)

    def __buildDeadlineJobWalk(self, batch, dispatch_data):
        if batch.blindData().get("deadlineDispatcher:visited"):
            return self.__getGafferDeadlineJob(batch.node(), batch.context())

        deadline_job = self.__getGafferDeadlineJob(batch.node(), batch.context())
        if not deadline_job:
            deadline_job = GafferDeadline.GafferDeadlineJob()
            deadline_job.setGafferNode(batch.node())
            deadline_job.setContext(batch.context())
            deadline_job.setAuxFiles([dispatch_data["scriptFile"]])
            self.__addGafferDeadlineJob(deadline_job)

        deadline_job.addBatch(batch, batch.frames())
        for upstream_batch in batch.preTasks():
            parent_deadline_job = self.__buildDeadlineJobWalk(upstream_batch, dispatch_data)
            if parent_deadline_job is not None:
                deadline_job.addParentJob(parent_deadline_job)

        batch.blindData()["deadlineDispatcher:visited"] = IECore.BoolData(True)

        return deadline_job

    def __buildDeadlineDependencyWalk(self, job):
        for parent_job in job.getParentJobs():
            self.__buildDeadlineDependencyWalk(parent_job)
        job.buildTaskDependencies()

    def __removeOrphanTasksWalk(self, job):
        for parent_job in job.getParentJobs():
            self.__removeOrphanTasksWalk(parent_job)
        job.removeOrphanTasks()

    def __getGafferDeadlineJob(self, node, context):
        for j in self._deadline_jobs:
            if j.getGafferNode() == node and j.getContext() == context:
                return j

    def __addGafferDeadlineJob(self, new_deadline_job):
        self._deadline_jobs.append(new_deadline_job)
        self._deadline_jobs = list(set(self._deadline_jobs))

    def __submitDeadlineJob(self, deadline_job, dispatch_data):
        # submit jobs depth first so parent job IDs will be populated
        for parent_job in deadline_job.getParentJobs():
            self.__submitDeadlineJob(parent_job, dispatch_data)

        gaffer_node = deadline_job.getGafferNode()
        if gaffer_node is None or len(deadline_job.getTasks()) == 0 or len(deadline_job.getTasks()) == 0:
            return None

        # this job is already submitted if it has an ID
        if deadline_job.getJobID() is not None:
            return deadline_job.getJobID()

        self.preSpoolSignal()(self, deadline_job)

        deadline_plug = gaffer_node["dispatcher"].getChild("deadline")

        if deadline_plug is not None:
            initial_status = "Suspended" if deadline_plug["submitSuspended"].getValue() else "Active"
            machine_list_type = "Blacklist" if deadline_plug["isBlackList"].getValue() else "Whitelist"
            
            # to prevent Deadline from splitting up our tasks (since we've already done that based on batches), set the chunk size to the largest frame range
            chunk_size = deadline_job.getTasks()[0].getEndFrame() - deadline_job.getTasks()[0].getStartFrame() + 1
            frame_string = ""
            for t in deadline_job.getTasks():
                chunk_size = max(t.getEndFrame() - t.getStartFrame() + 1, chunk_size)
                if t.getStartFrame() == t.getEndFrame():
                    frame_string += ",{}".format(t.getStartFrame())
                else:
                    frame_string += ",{}-{}".format(t.getStartFrame(), t.getEndFrame())
            
            job_info = {"Name": gaffer_node.relativeName(dispatch_data["scriptNode"]),
                        "Frames": frame_string,
                        "ChunkSize": chunk_size,
                        "Plugin": "Gaffer",
                        "BatchName": dispatch_data["deadlineBatch"],
                        "Comment": deadline_plug["comment"].getValue(),
                        "Department": deadline_plug["department"].getValue(),
                        "Pool": deadline_plug["pool"].getValue(),
                        "SecondaryPool": deadline_plug["secondaryPool"].getValue(),
                        "Group": deadline_plug["group"].getValue(),
                        "Priority": deadline_plug["priority"].getValue(),
                        "TaskTimeoutMinutes": int(deadline_plug["taskTimeout"].getValue()),
                        "EnableAutoTimeout": deadline_plug["enableAutoTimeout"].getValue(),
                        "ConcurrentTasks": deadline_plug["concurrentTasks"].getValue(),
                        "MachineLimit": deadline_plug["machineLimit"].getValue(),
                        machine_list_type: deadline_plug["machineList"].getValue(),
                        "LimitGroups": deadline_plug["limits"].getValue(),
                        "OnJobComplete": deadline_plug["onJobComplete"].getValue(),
                        "InitialStatus": initial_status,
                        "EnvironmentKeyValue0": "IECORE_LOG_LEVEL=INFO",   # GafferVRay uses INFO log level for progress output
                        }

            """ Dependencies are stored with a reference to the Deadline job since job IDs weren't assigned
            when the task tree was walked. Now that parent jobs have been submitted and have IDs,
            we can substitute that in for the dependency script to pick up.

            We also want to dependencies to be as native to Deadline as possible, resorting to the dependency
            script only in cases where it is needed (Deadline's dependency script triggering seems to be slower
            than native task dependencies)

            There are three possible dependency types allowed by Deadline:
            1) Job-Job:     All of the tasks for job A wait for all of the tasks for job B to finish before job A runs.
                            This is relatively rare when coming from Deadline and mostly is used by nodes upstream from
                            a FrameMask node. In that case releasing tasks per-frame would trigger downstream jobs sooner
                            than they should.
            2) Frame-Frame: This is somewhat misleadingly named because Deadline only checks for frame dependency release 
                            after each task completes, so this is very similar to task-task dependencies. Deadline can 
                            only handle a start and end frame offset when comparing to the parent job so the task
                            offsets must match across all parent jobs to enable this mode.
            3) Task-Task:   A task for job A waits for a task for job B to finish before the task for job A runs.
                            If the dependency start and end frame offsets don't match, this has to be handled by a
                            dependency script.
            """
            dep_list = deadline_job.getDependencies()

            if len(dep_list) > 0 and deadline_plug["dependencyMode"].getValue() != "None":
                job_dependent = False
                frame_dependent = False
                simple_frame_offset = True
                if deadline_plug["dependencyMode"].getValue() == "Job":
                    job_dependent = True
                elif deadline_plug["dependencyMode"].getValue() == "Frame":
                    frame_dependent = True
                elif deadline_plug["dependencyMode"].getValue() == "Auto":
                    job_dependent = False
                    dep_jobs = [j["dependency_job"] for j in dep_list]
                    dep_jobs = list(set(dep_jobs))
                    for dep in dep_list:
                        if dep["dependency_task"].getStartFrame() is None or dep["dependency_task"].getEndFrame() is None:
                            job_dependent = True

                    frame_dependent = True
                    simple_frame_offset = True
                    if len(dep_list) > 0 and dep_list[0]["dependency_task"].getStartFrame() is not None and dep_list[0]["dependency_task"].getEndFrame() is not None:
                        deadline_job._frame_dependency_offset_start = dep_list[0]["dependency_task"].getStartFrame() - dep_list[0]["dependent_task"].getStartFrame()
                        deadline_job._frame_dependency_offset_end = dep_list[0]["dependency_task"].getEndFrame() - dep_list[0]["dependent_task"].getEndFrame()

                        for dep_task in dep_list:
                            new_frame_offset_start = dep_task["dependency_task"].getStartFrame() - dep_task["dependent_task"].getStartFrame()
                            new_frame_offset_end= dep_task["dependency_task"].getEndFrame() - dep_task["dependent_task"].getEndFrame()

                            if new_frame_offset_start != deadline_job._frame_dependency_offset_start or new_frame_offset_end != deadline_job._frame_dependency_offset_end:
                                simple_frame_offset = False
                        
                        # If we can't just shift the frame start and end, we might still be able to use frame dependency with tasks of different frame lengths
                        if not simple_frame_offset:
                            dep_jobs = [j["dependency_job"] for j in dep_list]
                            dep_jobs = list(set(dep_jobs))
                            for dep_job in dep_jobs:
                                dep_tasks = [t["dependency_task"] for t in dep_list if t["dependency_job"] == dep_job]
                                dep_frames = []
                                for t in dep_tasks:
                                    if t.getStartFrame() is not None and t.getEndFrame() is not None:
                                        dep_frames += range(t.getStartFrame(), t.getEndFrame() + 1)
                                current_tasks = [t["dependent_task"] for t in dep_list if t["dependency_job"] == dep_job]
                                current_frames = []
                                for t in current_tasks:
                                    if t.getStartFrame() is not None and t.getEndFrame() is not None:
                                        current_frames += range(t.getStartFrame(), t.getEndFrame() + 1)
                                for f in current_frames:
                                    if f not in dep_frames:
                                        frame_dependent = False
                                
                    else:
                        frame_dependent = False

                if job_dependent or frame_dependent:
                    job_info.update(
                        {
                            "JobDependencies": ",".join(list(set([j["dependency_job"].getJobID() for j in dep_list]))),
                            "ResumeOnDeletedDependencies": True,
                        }
                    )
                    if simple_frame_offset:
                        job_info.update(
                            {
                                "FrameDependencyOffsetStart": deadline_job._frame_dependency_offset_start,
                                "FrameDependencyOffsetEnd": deadline_job._frame_dependency_offset_end,
                            }
                        )
                    if frame_dependent:
                        job_info.update({"IsFrameDependent": True})
                        deadline_job.setDependencyType(GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.frame_to_frame)
                    else:
                        deadline_job.setDependencyType(GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.job_to_job)
                else:
                    job_info.update(
                        {
                            "ScriptDependencies": os.environ["DEADLINE_DEPENDENCY_SCRIPT_PATH"],
                            "IsFrameDependent": True,
                        }
                    )
                    for i in range(0,len(dep_list)):
                        dep_task = dep_list[i]
                        job_info["ExtraInfoKeyValue{}".format(i)] = "{}:{}={}".format(int(dep_task["dependent_task"].getTaskNumber()), dep_task["dependency_job"].getJobID(), dep_task["dependency_task"].getTaskNumber())
                    deadline_job.setDependencyType(GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.scripted)
            else:
                deadline_job.setDependencyType(GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.none)

            plugin_info = {"Script": os.path.split(dispatch_data["scriptFile"])[-1],
                           "Version": Gaffer.About.versionString(),
                           "IgnoreScriptLoadErrors": False,
                           "Nodes": gaffer_node.relativeName(dispatch_data["scriptNode"]),
                           "Frames": "<STARTFRAME>-<ENDFRAME>",
                           }
            scriptContext = dispatch_data["scriptNode"].context()
            contextArgs = []
            for entry in [k for k in deadline_job.getContext().keys() if k != "frame" and not k.startswith("ui:")]:
                if entry not in scriptContext.keys() or deadline_job.getContext()[entry] != scriptContext[entry]:
                    contextArgs.extend(["\"-{}\"".format(entry), "\"{}\"".format(repr(deadline_job.getContext()[entry]))])
            if contextArgs:
                plugin_info["Context"] = " ".join(contextArgs)
            deadline_job.setJobProperties(job_info)
            deadline_job.setPluginProperties(plugin_info)
            job_file_path = os.path.join(os.path.split(dispatch_data["scriptFile"])[0], gaffer_node.relativeName(dispatch_data["scriptNode"]) + ".job")
            plugin_file_path = os.path.join(os.path.split(dispatch_data["scriptFile"])[0], gaffer_node.relativeName(dispatch_data["scriptNode"]) + ".plugin")

            job_id, output = deadline_job.submitJob(job_file_path=job_file_path, plugin_file_path=plugin_file_path)
            if job_id is None:
                IECore.Log.error(job_info["Name"], "failed to submit to Deadline.", output)
            else:
                IECore.Log.info(job_info["Name"], "submission succeeded.", output)

            return deadline_job.getJobID()
        else:
            print "oh no!"
            return None

    @staticmethod
    def _setupPlugs(parent_plug):

        if "deadline" in parent_plug:
            return

        parent_plug["deadline"] = Gaffer.Plug()
        parent_plug["deadline"]["comment"] = Gaffer.StringPlug()
        parent_plug["deadline"]["department"] = Gaffer.StringPlug()
        parent_plug["deadline"]["pool"] = Gaffer.StringPlug()
        parent_plug["deadline"]["secondaryPool"] = Gaffer.StringPlug()
        parent_plug["deadline"]["group"] = Gaffer.StringPlug()
        parent_plug["deadline"]["priority"] = Gaffer.IntPlug(defaultValue=50, minValue=0, maxValue=100)
        parent_plug["deadline"]["taskTimeout"] = Gaffer.IntPlug(defaultValue=0, minValue=0)
        parent_plug["deadline"]["enableAutoTimeout"] = Gaffer.BoolPlug(defaultValue=False)
        parent_plug["deadline"]["concurrentTasks"] = Gaffer.IntPlug(defaultValue=1, minValue=1, maxValue=16)
        parent_plug["deadline"]["machineLimit"] = Gaffer.IntPlug(defaultValue=0, minValue=0)
        parent_plug["deadline"]["machineList"] = Gaffer.StringPlug()
        parent_plug["deadline"]["isBlackList"] = Gaffer.BoolPlug(defaultValue=False)
        parent_plug["deadline"]["limits"] = Gaffer.StringPlug()
        parent_plug["deadline"]["onJobComplete"] = Gaffer.StringPlug()
        parent_plug["deadline"]["onJobComplete"].setValue("Nothing")
        parent_plug["deadline"]["submitSuspended"] = Gaffer.BoolPlug(defaultValue=False)
        parent_plug["deadline"]["dependencyMode"] = Gaffer.StringPlug()
        parent_plug["deadline"]["dependencyMode"].setValue("Auto")

IECore.registerRunTimeTyped(DeadlineDispatcher, typeName="GafferDeadline::DeadlineDispatcher")

GafferDispatch.Dispatcher.registerDispatcher("Deadline", DeadlineDispatcher, DeadlineDispatcher._setupPlugs)
