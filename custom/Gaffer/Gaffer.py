#!/usr/bin/env python3
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
    Progress = 0
    CurrFrame = 0
    
    def __init__(self):
        super().__init__()

        self.InitializeProcessCallback += self.InitializeProcess
        # self.RenderTasksCallback += self.RenderTasks
        self.RenderExecutableCallback += self.GetRenderExecutable
        self.RenderArgumentCallback += self.GetRenderArguments
        self.PreRenderTasksCallback += self.PreRenderTasks
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

        # Generic Gaffer progress and error
        self.AddStdoutHandlerCallback(".*Progress: (\d+)%.*").HandleCallback += self.HandleProgress

        # Alfred style progress from Houdini
        self.AddStdoutHandlerCallback(".*ALF_PROGRESS\s*(\d+)%.*").HandleCallback += self.HandleProgress

        # Vray's ply2vrmesh prints out lines for each frame and also each voxel within the frame
        self.AddStdoutHandlerCallback(".*Subdividing frame ([0-9]+) of ([0-9]+).*").HandleCallback += self.HandlePly2VrmeshFrameProgress
        self.AddStdoutHandlerCallback(".*Processing voxel ([0-9]+) of ([0-9]+).*").HandleCallback += self.HandlePly2VrmeshVoxelProgress

        # Arnold progress
        self.AddStdoutHandlerCallback("([0-9]+)% done").HandleCallback += self.HandleArnoldProgress

        # GafferVRay progress matches that of VRay
        self.AddStdoutHandlerCallback("error:.*").HandleCallback += self.HandleGafferVRayStdoutError
        self.AddStdoutHandlerCallback(".*Rendering image.*:\s*([0-9]*\.[0-9]*)%.*").HandleCallback += self.HandleGafferVRayStdoutProgress
        self.AddStdoutHandlerCallback(".*Rendering image...: done.*").HandleCallback += self.HandleGafferVRayStdoutComplete
        self.AddStdoutHandlerCallback(".*Starting frame ([0-9]*).*").HandleCallback += self.HandleGafferVRayStdoutStartFrame
        self.AddStdoutHandlerCallback(".*Closing log.*").HandleCallback += self.HandleGafferVRayStdoutClosing
        self.AddStdoutHandlerCallback( ".*Frame took.*" ).HandleCallback += self.HandleGafferVRayStdoutClosing

        self.SetEnvironmentVariable("AUXFILEDIRECTORY", self.GetJobsDataDirectory())
        if self.OverrideGpuAffinity():
            self.SetEnvironmentVariable("GPUAFFINITY", ",".join([str(i) for i in self.GpuAffinity()]))
        self.SetEnvironmentVariable("CPUTHREAD", str(self.GetThreadNumber()))

    def PreRenderTasks(self):
        self.LogInfo("Performing path mapping")

        script = RepositoryUtils.CheckPathMapping(self.GetPluginInfoEntryWithDefault("Script", "").strip())
        script = self.replaceSlashesByOS(script)
        localScript = os.path.join(self.GetJobsDataDirectory(), script)
        if not os.path.isfile(localScript):
            self.FailRender("Could not find Gaffer script {}".format(localScript))

        tempSceneDirectory = self.CreateTempDirectory("thread" + str(self.GetThreadNumber()))
        tempSceneFilename = Path.Combine(tempSceneDirectory, Path.GetFileName(localScript))

        with open(localScript, "r", encoding="utf-8") as inFile, open(tempSceneFilename, "w", encoding="utf-8") as outFile:
            for line in inFile:
                newLine = RepositoryUtils.CheckPathMapping(line)
                outFile.write(newLine)
        
        self._gafferScript = tempSceneFilename

    def GetRenderExecutable(self):
        self.Version = self.GetPluginInfoEntry("Version")
        gafferExeList = self.GetConfigEntry("Executable" + str(self.Version).replace(".", "_"))
        gafferExe = FileUtils.SearchFileList(gafferExeList)
        if(gafferExe == ""):
            self.FailRender("Gaffer %s render executable could not be found in the semicolon separated list \"%s\". The path to the render executable can be configured from the Plugin Configuration in the Deadline Monitor." % (
                self.Version, gafferExeList))

        return gafferExe

    def GetRenderArguments(self):
        script = self._gafferScript

        if not os.path.isfile(script):
            self.FailRender("Could not find path mapped Gaffer script {}".format(script))

        ignoreErrors = self.GetPluginInfoEntryWithDefault("IgnoreScriptLoadErrors", "False")
        nodes = self.GetPluginInfoEntryWithDefault("Nodes", "")
        frames = self.GetPluginInfoEntryWithDefault("Frames", "")
        frames = re.sub(r"<(?i)STARTFRAME>", str(self.GetStartFrame()), frames)
        frames = re.sub(r"<(?i)ENDFRAME>", str(self.GetEndFrame()), frames)
        frames = self.ReplacePaddedFrame(frames, "<(?i)STARTFRAME%([0-9]+)>", self.GetStartFrame())
        frames = self.ReplacePaddedFrame(frames, "<(?i)ENDFRAME%([0-9]+)>", self.GetEndFrame())
        context = self.GetPluginInfoEntryWithDefault("Context", "")

        arguments = "execute"

        threads = self.GetIntegerPluginInfoEntryWithDefault("Threads", 0)
        if self.OverrideCpuAffinity():
            threads = min(threads, len(self.CpuAffinity())) if threads != 0 else len(self.CpuAffinity())
        if threads != 0:
            arguments += " -threads {}".format(threads)

        arguments += " -script \"{}\"".format(script)
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

    def HandleGafferError(self):
        self.FailRender(self.GetRegexMatch(0))
        self.UpdateProgress()

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

    # GafferVRay Out Handlers
    def HandleGafferVRayStdoutError(self):
        self.FailRender(self.GetRegexMatch(0))
        self.UpdateProgress()

    def HandleGafferVRayStdoutProgress(self):
        self.Progress = float(self.GetRegexMatch(1))
        self.UpdateProgress()

    def HandleGafferVRayStdoutComplete(self):
        self.Progress = 100
        self.UpdateProgress()

    def HandleGafferVRayStdoutStartFrame(self):
        self.CurrFrame = float(self.GetRegexMatch(1))
        self.SetStatusMessage("Rendering Frame - " + self.GetRegexMatch(1))

    def HandleGafferVRayStdoutClosing(self):
        self.SetStatusMessage("Job Complete")

    def HandleArnoldProgress(self):
        self.Progress = float(self.GetRegexMatch(1))
        self.UpdateProgress()

    # Helper Functions
    def UpdateProgress(self):
        if((self.GetStartFrame() - self.GetEndFrame()) == 0):
            self.SetProgress(self.Progress)
        else:
            self.SetProgress((((1.0 / (self.GetEndFrame() - self.GetStartFrame() + 1))) * self.Progress) + (
                (((self.CurrFrame - self.GetStartFrame()) * 1.0) / (((self.GetEndFrame() - self.GetStartFrame() + 1) * 1.0))) * 100))

    def replaceSlashesByOS(self, value):
        if SystemUtils.IsRunningOnWindows():
            value = value.replace('/', '\\')
        else:
            value = value.replace("\\", "/")

        return value
