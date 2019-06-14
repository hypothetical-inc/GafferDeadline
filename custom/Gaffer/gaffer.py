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
import re

from System.IO import *
from System.Text.RegularExpressions import *

from Deadline.Scripting import *
from Deadline.Plugins import *

from FranticX.Processes import *

######################################################################
# This is the function that Deadline calls to get an instance of the
# main DeadlinePlugin class.
######################################################################


def GetDeadlinePlugin():
    return GafferPlugin()


def CleanupDeadlinePlugin(deadlinePlugin):
    deadlinePlugin.Cleanup()

######################################################################
# This is the main DeadlinePlugin class for the Gaffer plugin.
######################################################################


class GafferPlugin(DeadlinePlugin):
    def __init__(self):
        self.InitializeProcessCallback += self.InitializeProcess
        # self.RenderTasksCallback += self.RenderTasks
        self.RenderExecutableCallback += self.GetRenderExecutable
        self.RenderArgumentCallback += self.GetRenderArguments
        # Some tasks like Ply2Vrmesh and Houdini sims handle multiple frames rather than a separate Deadline task per frame
        self.currentFrame = 0.0
        self.totalFrames = 0.0

    def Cleanup(self):
        for stdoutHandler in self.StdoutHandlers:
            del stdoutHandler.HandleCallback

        del self.InitializeProcessCallback
        del self.RenderTasksCallback

    def InitializeProcess(self):
        self.PluginType = PluginType.Simple
        self.StdoutHandling = True

        # Generic Gaffer progress
        self.AddStdoutHandlerCallback(".*Progress: (\d+)%.*").HandleCallback += self.HandleProgress

        # Vray's ply2vrmesh prints out lines for each frame and also each voxel within the frame
        self.AddStdoutHandlerCallback(".*Subdividing frame ([0-9]+) of ([0-9]+).*").HandleCallback += self.HandlePly2VrmeshFrameProgress
        self.AddStdoutHandlerCallback(".*Processing voxel ([0-9]+) of ([0-9]+).*").HandleCallback += self.HandlePly2VrmeshVoxelProgress

    def GetRenderExecutable(self):
        self.Version = self.GetPluginInfoEntry("Version")
        gafferExeList = self.GetConfigEntry("Executable" + str(self.Version).replace(".", "_"))
        gafferExe = FileUtils.SearchFileList(gafferExeList)
        if(gafferExe == ""):
            self.FailRender("Gaffer %s render executable could not be found in the semicolon separated list \"%s\". The path to the render executable can be configured from the Plugin Configuration in the Deadline Monitor." % (
                self.Version, gafferExeList))

        return gafferExe

    def GetRenderArguments(self):
        script = RepositoryUtils.CheckPathMapping(self.GetPluginInfoEntryWithDefault("Script", "").strip())
        script = self.replaceSlashesByOS(script)
        local_script = os.path.join(self.GetJobsDataDirectory(), script)
        if os.path.isfile(local_script):
            script = local_script

        ignoreErrors = self.GetPluginInfoEntryWithDefault("IgnoreScriptLoadErrors", "False")
        nodes = self.GetPluginInfoEntryWithDefault("Nodes", "")
        frames = self.GetPluginInfoEntryWithDefault("Frames", "")
        frames = re.sub(r"<(?i)STARTFRAME>", str(self.GetStartFrame()), frames)
        frames = re.sub(r"<(?i)ENDFRAME>", str(self.GetEndFrame()), frames)
        frames = self.ReplacePaddedFrame(frames, "<(?i)STARTFRAME%([0-9]+)>", self.GetStartFrame())
        frames = self.ReplacePaddedFrame(frames, "<(?i)ENDFRAME%([0-9]+)>", self.GetEndFrame())
        context = self.GetPluginInfoEntryWithDefault("Context", "")

        arguments = "execute -script \"{}\"".format(script)
        arguments += " -ignoreScriptLoadErrors" if ignoreErrors.lower() == "true" else ""
        arguments += " -nodes {}".format(nodes) if nodes != "" else ""
        arguments += " -frames {}".format(frames) if frames != "" else ""
        arguments += " -context {}".format(context) if context != "" else ""

        return arguments

    def ReplacePaddedFrame(self, arguments, pattern, frame):
        frameRegex = Regex(pattern)
        while True:
            frameMatch = frameRegex.Match(arguments)
            if frameMatch.Success:
                paddingSize = int(frameMatch.Groups[1].Value)
                if paddingSize > 0:
                    padding = StringUtils.ToZeroPaddedString(frame, paddingSize, False)
                else:
                    padding = str(frame)
                arguments = arguments.replace(frameMatch.Groups[0].Value, padding)
            else:
                break

        return arguments

    def HandleProgress(self):
        progress = float(self.GetRegexMatch(1))
        self.SetProgress(progress)
     
    def HandlePly2VrmeshFrameProgress(self):
        self.currentFrame = float(self.GetRegexMatch(1)) - 1.0
        self.totalFrames = float(self.GetRegexMatch(2))

        self.SetProgress(self.currentFrame / self.totalFrames * 100)
        self.SetStatusMessage("Ply2Vrmesh: frame {}/{}".format(self.currentFrame, self.totalFrames))
        
    def HandlePly2VrmeshVoxelProgress(self):
        currentVoxel = float(self.GetRegexMatch(1)) - 1.0
        totalVoxels = float(self.GetRegexMatch(2))

        voxelProgress = currentVoxel / totalVoxels

        self.SetProgress(((self.currentFrame / self.totalFrames) + (voxelProgress * 1.0 / self.totalFrames)) * 100)
        self.SetStatusMessage("Ply2Vrmesh: Processing Voxel {}/{} @ frame {}/{}".format(currentVoxel, totalVoxels, self.currentFrame, self.totalFrames))

    def replaceSlashesByOS(self, value):
        if SystemUtils.IsRunningOnWindows():
            value = value.replace('/', '\\')
        else:
            value = value.replace("\\", "/")

        return value
