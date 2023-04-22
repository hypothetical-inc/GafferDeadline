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
import unittest

import Gaffer
import GafferTest
import GafferDeadline
import GafferDispatch
import GafferDispatchTest


class GafferDeadlineJobTest(GafferTest.TestCase):
    def testJobProperties(self):
        dj = GafferDeadline.GafferDeadlineJob(GafferDispatchTest.LoggingTaskNode())
        self.assertIn("Plugin", dj.getJobProperties())
        dj.setJobProperties({"testProp": "testVal"})
        self.assertIn("Plugin", dj.getJobProperties())
        self.assertIn("testProp", dj.getJobProperties())
        self.assertEqual(dj.getJobProperties()["testProp"], "testVal")

        dj = GafferDeadline.GafferDeadlineJob(
            GafferDispatchTest.LoggingTaskNode(),
            jobProperties={"testProp2": "testVal2"}
        )
        self.assertIn("Plugin", dj.getJobProperties())
        self.assertIn("testProp2", dj.getJobProperties())
        self.assertEqual(dj.getJobProperties()["testProp2"], "testVal2")

    def testPluginProperties(self):
        dj = GafferDeadline.GafferDeadlineJob(GafferDispatchTest.LoggingTaskNode())
        self.assertEqual(dj.getPluginProperties(), {})
        dj.setPluginProperties({"testProp": "testVal"})
        self.assertIn("testProp", dj.getPluginProperties())
        self.assertEqual(dj.getPluginProperties()["testProp"], "testVal")

        dj = GafferDeadline.GafferDeadlineJob(
            GafferDispatchTest.LoggingTaskNode(),
            pluginProperties={"testProp2": "testVal2"}
        )
        self.assertIn("testProp2", dj.getPluginProperties())
        self.assertEqual(dj.getPluginProperties()["testProp2"], "testVal2")

    def testAuxFiles(self):
        dj = GafferDeadline.GafferDeadlineJob(GafferDispatchTest.LoggingTaskNode())
        self.assertEqual(dj.getAuxFiles(), [])
        dj.setAuxFiles(["file1", "file2"])
        self.assertEqual(len(dj.getAuxFiles()), 2)
        self.assertIn("file1", dj.getAuxFiles())

        dj = GafferDeadline.GafferDeadlineJob(
            GafferDispatchTest.LoggingTaskNode(),
            auxFiles=["file3", "file4"]
        )
        self.assertEqual(len(dj.getAuxFiles()), 2)
        self.assertIn("file3", dj.getAuxFiles())

    def testGafferNode(self):
        taskNode = GafferDispatch.TaskNode()
        dj = GafferDeadline.GafferDeadlineJob(taskNode)
        self.assertEqual(dj.getGafferNode(), taskNode)

        self.assertRaises(ValueError, dj.setGafferNode, "bad value")

    def testAddBatch(self):
        dj = GafferDeadline.GafferDeadlineJob(GafferDispatchTest.LoggingTaskNode())
        dj.addBatch(None, [1, 2, 3, 4, 5])
        dj.addBatch(None, [6, 7, 8, 9, 10])
        self.assertEqual(dj._tasks[0].getStartFrame(), 1)
        self.assertEqual(dj._tasks[0].getEndFrame(), 5)
        self.assertEqual(dj._tasks[1].getStartFrame(), 6)
        self.assertEqual(dj._tasks[1].getEndFrame(), 10)

        dj = GafferDeadline.GafferDeadlineJob(GafferDispatchTest.LoggingTaskNode())
        dj.addBatch(None, [1, 2, 3, 7, 8, 9, 100.0, 101.0, 102.0])
        self.assertEqual(len(dj._tasks), 3)
        self.assertEqual(dj._tasks[0].getTaskNumber(), 0)
        self.assertEqual(dj._tasks[0].getStartFrame(), 1)
        self.assertEqual(dj._tasks[0].getEndFrame(), 3)
        self.assertEqual(dj._tasks[1].getTaskNumber(), 1)
        self.assertEqual(dj._tasks[1].getStartFrame(), 7)
        self.assertEqual(dj._tasks[1].getEndFrame(), 9)
        self.assertEqual(dj._tasks[2].getTaskNumber(), 2)
        self.assertEqual(dj._tasks[2].getStartFrame(), 100)
        self.assertEqual(dj._tasks[2].getEndFrame(), 102)

    def testContext(self):
        dj = GafferDeadline.GafferDeadlineJob(GafferDispatchTest.LoggingTaskNode())
        self.assertEqual(dj.getContext(), Gaffer.Context())
        dj.setContext({"test": "value"})
        self.assertEqual(dj.getContext(), {"test": "value"})

    def testParentJob(self):
        taskNode = GafferDispatch.TaskNode()
        taskNode2 = GafferDispatch.TaskNode()
        djc = GafferDeadline.GafferDeadlineJob(taskNode)
        djp = GafferDeadline.GafferDeadlineJob(taskNode2)
        djc.addParentJob(djp)
        self.assertIn(djp, djc.getParentJobs())
        djp.setGafferNode(taskNode)
        self.assertEqual(djc.getParentJobByGafferNode(taskNode), djp)
        self.assertEqual(djc.getParentJobByGafferNode(taskNode2), None)

    def testOutputs(self):
        dj = GafferDeadline.GafferDeadlineJob(GafferDispatchTest.LoggingTaskNode())

        self.assertEqual(dj.getOutputs(), [])

        context = Gaffer.Context()
        context["putMeInCoach"] = "rudyRudyRudy"

        for input, output in [
            ("test/path.exr", "test/path.exr"),
            ("test/path.####.exr", "test/path.####.exr"),
            ("${putMeInCoach}/path.####.exr", "rudyRudyRudy/path.####.exr")
        ]:
            self.assertIn("frame", context)
            dj.addOutput(input, context)
            self.assertIn(output, dj.getOutputs())

        self.assertEqual(len(dj.getOutputs()), 3)

        dj.clearOutputs()
        self.assertEqual(len(dj.getOutputs()), 0)


if __name__ == "__main__":
    unittest.main()
