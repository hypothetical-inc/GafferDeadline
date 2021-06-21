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

import re

from Deadline.Scripting import *

""" ExtraInfoKeyValue<xxx> denote dependecies in the form of
<job frame>:<dependent job id>=><dependent job task number>

Deadline doesn't seem to include a logging facility for dependency scripts
so just using print for informative info in case jobs aren't releasing
as expected.
"""

printDebug = False


def __main__(jobID, taskIDs=None):

    # simple data structure to hold information about a dependency
    class dependency(object):
        def __init__(self, task, jobDep, depTask, isReleased):
            self.taskId = int(task)
            self.jobDependencyId = jobDep
            self.dependencyTaskId = int(depTask)
            self.isReleased = isReleased

    taskIDs = [int(t) for t in taskIDs] # Deadline gives task IDs in string format

    if taskIDs:
        re_dep = re.compile(r'^([0-9]+):([a-z0-9]+)')

        job = RepositoryUtils.GetJob(jobID, False)
        if printDebug:
            print("Checking dependencies for {}".format(job.JobName))

        dependencies = []   # list of dependency objects
        jobDependencyIds = []   # list of job ids this job depends on

        # collect this job's dependencies
        for k in job.GetJobExtraInfoKeys():
            result = re_dep.match(k)
            if len(result.groups()) == 2:
                task, jobDep = result.groups()
                if int(task) in taskIDs:
                    taskDep = job.GetJobExtraInfoKeyValue(k)
                    newDep = dependency(task, jobDep, taskDep, False)
                    jobDependencyIds.append(jobDep)
                    dependencies.append(newDep)

        if printDebug: print("Found {} dependencies".format(len(dependencies)))

        jobDependencyIds = list(set(jobDependencyIds))
        # if no dependencies, release all tasks
        if len(dependencies) == 0:
            return taskIDs

        for jobDepId in jobDependencyIds:
            if printDebug: print "Scanning {} for released dependencies".format(jobDepId)
            jobDepObj = RepositoryUtils.GetJob(jobDep, False)
            # If the job can't be found, assume it is ok to release it's dependents
            if jobDepObj is None:
                for d in dependencies:
                    if d.jobDependencyId == jobDepId:
                        d.isReleased = True
            else:
                jobDepTaskList = RepositoryUtils.GetJobTasks(jobDepObj, False).TaskCollectionTasks
                completedTasks = [t for t in jobDepTaskList if t.TaskStatus.lower() == "completed"]
                if printDebug: print "{} has {} completed tasks of {} total tasks: {}".format(jobDepId, len(completedTasks), len(jobDepTaskList), [t.TaskId for t in completedTasks])
                for completedTask in completedTasks:
                    for d in dependencies:
                        if d.jobDependencyId == jobDepId and d.dependencyTaskId == int(completedTask.TaskId):
                            d.isReleased = True
                            print("{}:{} released".format(d.jobDependencyId, d.dependencyTaskId))

        releasedTasks = []
        for task in dependencies:
            if printDebug: print "Scanning task #{} for dependencies".format(task.taskId)
            depsForThisTask = list(set([t for t in dependencies if t.taskId == task.taskId]))
            # print "Task #{} has {} dependencies: {}".format(task.taskId, len(depsForThisTask), ",".join([d.dependencyTaskId for d in depsForThisTask]))
            releasedDeps = list(set([d for d in depsForThisTask if d.isReleased]))
            if printDebug: print "Task #{} has {} released dependencies".format(task.taskId, len(releasedDeps))
            if(len(depsForThisTask) == len(releasedDeps)):
                releasedTasks.append(str(task.taskId))
                if printDebug: print "All dependencies for task #{} have been completed. Releasing task #{}".format(task.taskId, task.taskId)

        if printDebug: print("Released tasks for {} = {}".format(jobID, releasedTasks))
        return list(set(releasedTasks))

    # not entirely sure what to do about a job that does not have frame dependencies enabled, that is considered an error state
    return False
