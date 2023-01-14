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
import subprocess
import re

import IECore


def runDeadlineCommand(arguments, hideWindow=True):
    if "DEADLINE_PATH" not in os.environ:
        raise RuntimeError("DEADLINE_PATH must be set to the Deadline executable path")
    executableSuffix = ".exe" if os.name == "nt" else ""
    deadlineCommand = os.path.join(
        os.environ['DEADLINE_PATH'],
        "deadlinecommand" + executableSuffix
    )

    arguments = [deadlineCommand] + arguments

    p = subprocess.Popen(arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    output, err = p.communicate()

    if err:
        raise RuntimeError(
            "Error running Deadline command {}: {}".format(
                " ".join(arguments),
                output
            )
        )

    return output


def submitJob(jobInfoFile, pluginInfoFile, auxFiles):
    submissionResults = runDeadlineCommand(
        [jobInfoFile, pluginInfoFile] + [str(f) for f in auxFiles]
    )

    for i in submissionResults.split():
        line = i.decode()
        if line.startswith("JobID="):
            jobID = line.replace("JobID=", "").strip()
            return (jobID, submissionResults)

    return (None, submissionResults)


def getMachineList():
    output = runDeadlineCommand(["GetSlaveNames"])
    return [i.decode() for i in output.split()]


def getLimitGroups():
    output = runDeadlineCommand(["GetLimitGroups"])
    return re.findall(r'Name=(.*)', output.decode())


def getGroups():
    output = runDeadlineCommand(["GetSubmissionInfo", "groups"])
    return [i.decode() for i in output.split()[1:]]    # remove [Groups] header


def getPools():
    output = runDeadlineCommand(["GetSubmissionInfo", "pools"])
    return [i.decode() for i in output.split()[1:]]    # remove [Groups] header
