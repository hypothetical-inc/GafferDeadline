##########################################################################
#
#  Copyright (c) 2023, Hypothetical Inc. All rights reserved.
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

import unittest
from unittest import mock

import IECore

import Gaffer
import GafferDispatch
import GafferTest

import GafferDeadline


class DeadlineTaskTest(GafferTest.TestCase):
    def __dispatcher(self):
        dispatcher = GafferDeadline.DeadlineDispatcher()
        dispatcher["jobsDirectory"].setValue(self.temporaryDirectory() / "testJobDirectory")

        return dispatcher

    def __assertSettings(self, fileName, settings):
        with open(fileName) as file:
            fileSettings = {
                i.split("=", 1)[0]: i.strip().split("=", 1)[1] for i in file.readlines()
            }
        self.assertEqual(fileSettings, settings)

    def test(self):
        s = Gaffer.ScriptNode()
        s["n"] = GafferDeadline.DeadlineTask()
        s["n"]["plugin"].setValue("customPlugin")
        s["n"]["parameters"].addChild(
            Gaffer.NameValuePlug("stringSetting", IECore.StringData("value1"))
        )
        s["n"]["parameters"].addChild(Gaffer.NameValuePlug("intSetting", IECore.IntData(50)))
        s["n"]["parameters"].addChild(Gaffer.NameValuePlug("boolSetting", IECore.BoolData(True)))

        dispatcher = self.__dispatcher()
        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            dispatcher.dispatch([s["n"]])

        jobSettings = {
            "Name": "n",
            "Frames": ",1",
            "ChunkSize": "1",
            "Plugin": "customPlugin",
            "BatchName": "untitled",
            "Comment": "",
            "Department": "",
            "Pool": "",
            "SecondaryPool": "",
            "Group": "",
            "Priority": "50",
            "TaskTimeoutMinutes": "0",
            "EnableAutoTimeout": "False",
            "ConcurrentTasks": "1",
            "MachineLimit": "0",
            "Whitelist": "",
            "LimitGroups": "",
            "OnJobComplete": "Nothing",
            "InitialStatus": "Active",
            "EnvironmentKeyValue0": "IECORE_LOG_LEVEL=INFO",
        }

        self.__assertSettings(dispatcher.jobDirectory() / "n.job", jobSettings)
        self.__assertSettings(
            dispatcher.jobDirectory() / "n.plugin",
            {
                "stringSetting": "value1",
                "intSetting": "50",
                "boolSetting": "1",
            }
        )

        dispatcher["framesMode"].setValue(GafferDispatch.Dispatcher.FramesMode.FullRange)
        dispatcher["frameRange"].setValue("1-100")

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            dispatcher.dispatch([s["n"]])

        jobSettings["Frames"] = "," + ",".join([str(i) for i in range(1, 101)])

        self.__assertSettings(dispatcher.jobDirectory() / "n.job", jobSettings)

        s["n"]["dispatcher"]["batchSize"].setValue(10)

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            dispatcher.dispatch([s["n"]])

        jobSettings["ChunkSize"] = "10"
        jobSettings["Frames"] = ",1-10,11-20,21-30,31-40,41-50,51-60,61-70,71-80,81-90,91-100"

        self.__assertSettings(dispatcher.jobDirectory() / "n.job", jobSettings)


if __name__ == "__main__":
    unittest.main()
