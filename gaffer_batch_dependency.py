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

print_debug = False

def __main__(jobID, taskIDs=None):

    # simple data structure to hold information about a dependency
    class dependency(object):
        def __init__(self, task, job_dep, dep_task, is_released):
            self.task_id = int(task)
            self.job_dependency_id = job_dep
            self.dependency_task_id = int(dep_task)
            self.is_released = is_released

    taskIDs = [int(t) for t in taskIDs] # Deadline gives task IDs in string format

    if taskIDs:
        re_dep = re.compile(r'^([0-9]+):([a-z0-9]+)')

        job = RepositoryUtils.GetJob(jobID, False)
        if print_debug: print "Checking dependencies for {}".format(job.JobName)

        dependencies = []   # list of dependency objects
        job_dependency_ids = []   # list of job ids this job depends on

        # collect this job's dependencies
        for k in job.GetJobExtraInfoKeys():
            result = re_dep.match(k)
            if len(result.groups()) == 2:
                task, job_dep = result.groups()
                if int(task) in taskIDs:
                    task_dep = job.GetJobExtraInfoKeyValue(k)
                    new_dep = dependency(task, job_dep, task_dep, False)
                    job_dependency_ids.append(job_dep)
                    dependencies.append(new_dep)

        if print_debug: print("Found {} dependencies".format(len(dependencies)))

        job_dependency_ids = list(set(job_dependency_ids))
        # if no dependencies, release all tasks
        if len(dependencies) == 0:
            return taskIDs

        for job_dep_id in job_dependency_ids:
            if print_debug: print "Scanning {} for released dependencies".format(job_dep_id)
            job_dep_obj = RepositoryUtils.GetJob(job_dep, False)
            # If the job can't be found, assume it is ok to release it's dependents
            if job_dep_obj is None:
                for d in dependencies:
                    if d.job_dependency_id == job_dep_id:
                        d.is_released = True
            else:
                job_dep_task_list = RepositoryUtils.GetJobTasks(job_dep_obj, False).TaskCollectionTasks
                completed_tasks = [t for t in job_dep_task_list if t.TaskStatus.lower() == "completed"]
                if print_debug: print "{} has {} completed tasks of {} total tasks: {}".format(job_dep_id, len(completed_tasks), len(job_dep_task_list), [t.TaskId for t in completed_tasks])
                for completed_task in completed_tasks:
                    for d in dependencies:
                        if d.job_dependency_id == job_dep_id and d.dependency_task_id == int(completed_task.TaskId):
                            d.is_released = True
                            print("{}:{} released".format(d.job_dependency_id, d.dependency_task_id))

        released_tasks = []
        for task in dependencies:
            if print_debug: print "Scanning task #{} for dependencies".format(task.task_id)
            deps_for_this_task = list(set([t for t in dependencies if t.task_id == task.task_id]))
            # print "Task #{} has {} dependencies: {}".format(task.task_id, len(deps_for_this_task), ",".join([d.dependency_task_id for d in deps_for_this_task]))
            released_deps = list(set([d for d in deps_for_this_task if d.is_released]))
            if print_debug: print "Task #{} has {} released dependencies".format(task.task_id, len(released_deps))
            if(len(deps_for_this_task) == len(released_deps)):
                released_tasks.append(str(task.task_id))
                if print_debug: print "All dependencies for task #{} have been completed. Releasing task #{}".format(task.task_id, task.task_id)

        if print_debug: print("Released tasks for {} = {}".format(jobID, released_tasks))
        return list(set(released_tasks))

    # not entirely sure what to do about a job that does not have frame dependencies enabled, that is considered an error state
    return False
