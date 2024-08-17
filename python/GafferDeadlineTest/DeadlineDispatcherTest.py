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

import unittest
from unittest import mock

import IECore

import IECore

import Gaffer
import GafferTest
import GafferDispatch
import GafferDispatchTest

import GafferDeadline


class DeadlineDispatcherTest(GafferTest.TestCase):
    def __dispatcher(self):
        dispatcher = GafferDeadline.DeadlineDispatcher()
        dispatcher["jobsDirectory"].setValue(self.temporaryDirectory() / "testJobDirectory")

        return dispatcher

    def __job(self, nodes, dispatcher=None):
        jobs = []

        def f(dispatcher, job):
            jobs.append(job)

        c = GafferDeadline.DeadlineDispatcher.preSpoolSignal().connect(f, scoped=True)

        if dispatcher is None:
            dispatcher = self.__dispatcher()

        dispatcher.dispatch(nodes)

        return jobs

    def __debugPrintDependencies(self, dependencies):
        for d in dependencies.values():
            print(
                "{}-{} ->{}:{}-{}".format(
                    d.getDeadlineTask().getStartFrame(),
                    d.getDeadlineTask().getEndFrame(),
                    d.getDeadlineJob().getGafferNode().getName(),
                    d.getUpstreamDeadlineTask().getStartFrame(),
                    d.getUpstreamDeadlineTask().getEndFrame()
                )
            )

    def testPreSpoolSignal(self):
        s = Gaffer.ScriptNode()
        s["n"] = GafferDispatchTest.LoggingTaskNode()

        spooled = []

        def f(dispatcher, job):
            spooled.append((dispatcher, job))

        c = GafferDeadline.DeadlineDispatcher.preSpoolSignal().connect(f, scoped=True)

        dispatcher = self.__dispatcher()
        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            dispatcher.dispatch([s["n"]])

        self.assertEqual(len(spooled), 1)
        self.assertTrue(spooled[0][0] is dispatcher)

    def testTaskAttributes(self):
        s = Gaffer.ScriptNode()
        s["n"] = GafferDispatchTest.LoggingTaskNode()
        s["n"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n"]["dispatcher"]["batchSize"].setValue(10)

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-10")

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n"]], dispatcher)
        self.assertEqual(len(jobs[0].getParentJobs()), 0)
        self.assertEqual(len(jobs[0].getTasks()), 1)

    def testMultipleBatches(self):
        s = Gaffer.ScriptNode()
        s["n"] = GafferDispatchTest.LoggingTaskNode()
        s["n"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n"]["dispatcher"]["batchSize"].setValue(1)

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-10")

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n"]], dispatcher)

        self.assertEqual(len(jobs), 1)
        self.assertEqual(len(jobs[0].getParentJobs()), 0)
        self.assertEqual(len(jobs[0].getTasks()), 10)

    def testSingleBatch(self):
        s = Gaffer.ScriptNode()
        s["n"] = GafferDispatchTest.LoggingTaskNode()
        s["n"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n"]["dispatcher"]["batchSize"].setValue(10)

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-10")

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n"]], dispatcher)

        self.assertEqual(len(jobs), 1)
        self.assertEqual(len(jobs[0].getTasks()), 1)

    def testCollapseIdenticalFrames(self):
        s = Gaffer.ScriptNode()
        s["n"] = GafferDispatch.PythonCommand()
        s["n"]["command"] = Gaffer.StringPlug(
            defaultValue="print(\"Hello\")",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n"]["dispatcher"]["batchSize"].setValue(1)

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-10")

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n"]], dispatcher)

        self.assertEqual(len(jobs), 1)
        self.assertEqual(len(jobs[0].getTasks()), 1)

    def testPreTasks(self):
        # n1
        #  |
        # n2     n3

        s = Gaffer.ScriptNode()
        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n3"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["preTasks"][0].setInput(s["n1"]["task"])

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n2"], s["n3"]])

        self.assertEqual(len(jobs), 3)
        dependencyCount = {"n1": 0, "n2": 1, "n3": 0}
        self.assertEqual(
            len(jobs[0].getParentJobs()), dependencyCount[jobs[0].getJobProperties()["Name"]]
        )
        self.assertEqual(
            len(jobs[1].getParentJobs()), dependencyCount[jobs[1].getJobProperties()["Name"]]
        )
        self.assertEqual(
            len(jobs[2].getParentJobs()), dependencyCount[jobs[2].getJobProperties()["Name"]]
        )

    def testSharedPreTasks(self):
        #   n1
        #  / \
        # i1 i2
        #  \ /
        #   n2

        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["i1"] = GafferDispatchTest.LoggingTaskNode()
        s["i1"]["preTasks"][0].setInput(s["n1"]["task"])
        s["i2"] = GafferDispatchTest.LoggingTaskNode()
        s["i2"]["preTasks"][0].setInput(s["n1"]["task"])
        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["preTasks"][0].setInput(s["i1"]["task"])
        s["n2"]["preTasks"][1].setInput(s["i2"]["task"])

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n2"]])

        self.assertEqual(len(jobs), 4)
        self.assertEqual(len(jobs[-1].getParentJobs()), 2)
        self.assertEqual(len(jobs[-1].getParentJobs()[0].getParentJobs()), 1)
        self.assertEqual(len(jobs[-1].getParentJobs()[0].getParentJobs()[0].getParentJobs()), 0)

    def testOverrideNone(self):
        #   n1
        #   |
        #   n2

        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n1"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n1"]["dispatcher"]["batchSize"].setValue(10)

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n2"]["dispatcher"]["batchSize"].setValue(13)
        s["n2"]["dispatcher"]["deadline"]["dependencyMode"].setValue("None")
        s["n2"]["preTasks"][0].setInput(s["n1"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n2"]], dispatcher)

        self.assertEqual(len(jobs), 2)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n2":
                self.assertEqual(len(j.getDependencies()), 8)
                self.assertEqual(len(j.getTasks()), 4)
                self.assertEqual(
                    j.getDependencyType(),
                    GafferDeadline.GafferDeadlineJob.DeadlineDependencyType._None
                )
            elif j.getJobProperties()["Name"] == "n1":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 5)
                self.assertEqual(
                    j.getDependencyType(),
                    GafferDeadline.GafferDeadlineJob.DeadlineDependencyType._None
                )

    def testOverrideJob(self):
        #   n1
        #   |
        #   n2

        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n1"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n1"]["dispatcher"]["batchSize"].setValue(10)

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n2"]["dispatcher"]["batchSize"].setValue(13)
        s["n2"]["dispatcher"]["deadline"]["dependencyMode"].setValue("Job")
        s["n2"]["preTasks"][0].setInput(s["n1"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n2"]], dispatcher)

        self.assertEqual(len(jobs), 2)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n2":
                self.assertEqual(len(j.getDependencies()), 8)
                self.assertEqual(len(j.getTasks()), 4)
                self.assertEqual(
                    j.getDependencyType(),
                    GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.JobToJob
                )
            elif j.getJobProperties()["Name"] == "n1":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 5)
                self.assertEqual(
                    j.getDependencyType(),
                    GafferDeadline.GafferDeadlineJob.DeadlineDependencyType._None
                )

    def testOverrideFrame(self):
        #   n1
        #   |
        #   n2

        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n1"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n1"]["dispatcher"]["batchSize"].setValue(10)

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n2"]["dispatcher"]["batchSize"].setValue(13)
        s["n2"]["dispatcher"]["deadline"]["dependencyMode"].setValue("Frame")
        s["n2"]["preTasks"][0].setInput(s["n1"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n2"]], dispatcher)

        self.assertEqual(len(jobs), 2)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n2":
                # self.__debugPrintDependencies(j.getDependencies())
                self.assertEqual(len(j.getDependencies()), 8)
                self.assertEqual(len(j.getTasks()), 4)
                self.assertEqual(
                    j.getDependencyType(),
                    GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.FrameToFrame
                )
            elif j.getJobProperties()["Name"] == "n1":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 5)
                self.assertEqual(
                    j.getDependencyType(),
                    GafferDeadline.GafferDeadlineJob.DeadlineDependencyType._None
                )

    def testOverrideScript(self):
        #   n1
        #   |
        #   n2

        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n1"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n1"]["dispatcher"]["batchSize"].setValue(10)

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n2"]["dispatcher"]["batchSize"].setValue(13)
        s["n2"]["dispatcher"]["deadline"]["dependencyMode"].setValue("Script")
        s["n2"]["preTasks"][0].setInput(s["n1"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n2"]], dispatcher)

        self.assertEqual(len(jobs), 2)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n2":
                self.assertEqual(len(j.getDependencies()), 8)
                self.assertEqual(len(j.getTasks()), 4)
                self.assertEqual(
                    j.getDependencyType(),
                    GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.Scripted
                )
            elif j.getJobProperties()["Name"] == "n1":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 5)
                self.assertEqual(
                    j.getDependencyType(),
                    GafferDeadline.GafferDeadlineJob.DeadlineDependencyType._None
                )

    def testSequence(self):
        s = Gaffer.ScriptNode()

        s["n"] = GafferDispatch.PythonCommand()
        s["n"]["command"] = Gaffer.StringPlug(
            defaultValue="for i in frames:\n\tcontext.setFrame(i)\n\tprint(context.getFrame())",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n"]["sequence"].setValue(True)
        s["n"]["dispatcher"]["batchSize"].setValue(1)

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n"]], dispatcher)

        self.assertEqual(len(jobs), 1)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 1)

    def testDot(self):
        #   n1
        #   |
        #   d1
        #   |
        #   n2

        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n1"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n1"]["dispatcher"]["batchSize"].setValue(10)

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n2"]["dispatcher"]["batchSize"].setValue(13)

        s["d1"] = Gaffer.Dot()
        s["d1"].setup(s["n2"]["preTasks"][0])
        s["d1"]["in"].setInput(s["n1"]["task"])
        s["n2"]["preTasks"][0].setInput(s["d1"]["out"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n2"]], dispatcher)

        self.assertEqual(len(jobs), 2)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n2":
                self.assertEqual(len(j.getDependencies()), 8)
                self.assertEqual(len(j.getTasks()), 4)
            elif j.getJobProperties()["Name"] == "n1":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 5)
                self.assertEqual(
                    j.getDependencyType(),
                    GafferDeadline.GafferDeadlineJob.DeadlineDependencyType._None
                )

    def testDependencies(self):
        #   n1
        #  / \
        # i1 i2
        #  \ /
        #   n2

        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n1"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n1"]["dispatcher"]["batchSize"].setValue(10)

        s["i1"] = GafferDispatchTest.LoggingTaskNode()
        s["i1"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["i1"]["dispatcher"]["batchSize"].setValue(25)
        s["i1"]["preTasks"][0].setInput(s["n1"]["task"])

        s["i2"] = GafferDispatchTest.LoggingTaskNode()
        s["i2"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["i2"]["dispatcher"]["batchSize"].setValue(1)
        s["i2"]["preTasks"][0].setInput(s["n1"]["task"])

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n2"]["dispatcher"]["batchSize"].setValue(13)
        s["n2"]["preTasks"][0].setInput(s["i1"]["task"])
        s["n2"]["preTasks"][1].setInput(s["i2"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n2"]], dispatcher)

        self.assertEqual(len(jobs), 4)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n2":
                self.assertEqual(len(j.getDependencies()), 55)
                self.assertEqual(len(j.getTasks()), 4)
            elif j.getJobProperties()["Name"] == "i2":
                self.assertEqual(len(j.getDependencies()), 50)
                self.assertEqual(len(j.getTasks()), 50)
            elif j.getJobProperties()["Name"] == "i1":
                self.assertEqual(len(j.getDependencies()), 6)
                self.assertEqual(len(j.getTasks()), 2)
            elif j.getJobProperties()["Name"] == "n1":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 5)

    def testControlNodes(self):
        # t = TaskList node
        #   n1
        #   |
        #   n2  n3
        #    \ /
        # n4 t1
        #  \ /
        #   t2

        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n1"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n1"]["dispatcher"]["batchSize"].setValue(10)

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n2"]["dispatcher"]["batchSize"].setValue(25)
        s["n2"]["preTasks"][0].setInput(s["n1"]["task"])

        s["n3"] = GafferDispatchTest.LoggingTaskNode()
        s["n3"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n3"]["dispatcher"]["batchSize"].setValue(1)

        s["n4"] = GafferDispatchTest.LoggingTaskNode()
        s["n4"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n4"]["dispatcher"]["batchSize"].setValue(1)

        s["t1"] = GafferDispatch.TaskList()
        s["t1"]["dispatcher"]["batchSize"].setValue(1)
        s["t1"]["preTasks"][0].setInput(s["n2"]["task"])
        s["t1"]["preTasks"][1].setInput(s["n3"]["task"])

        s["t2"] = GafferDispatch.TaskList()
        s["t2"]["dispatcher"]["batchSize"].setValue(1)
        s["t2"]["preTasks"][0].setInput(s["n4"]["task"])
        s["t2"]["preTasks"][1].setInput(s["t1"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["t2"]], dispatcher)

        self.assertEqual(len(jobs), 4)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n4":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 50)
            if j.getJobProperties()["Name"] == "n3":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 50)
            if j.getJobProperties()["Name"] == "n2":
                self.assertEqual(len(j.getDependencies()), 6)
                self.assertEqual(len(j.getTasks()), 2)
            if j.getJobProperties()["Name"] == "n1":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 5)

    def testMultipleDependency(self):
        #    n1
        #   / \
        #  n2  n3
        #  |   |
        #  n4  |
        #   \ /
        #    t1
        #    |
        #    n5
        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n1"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n1"]["dispatcher"]["batchSize"].setValue(1)

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n2"]["dispatcher"]["batchSize"].setValue(15)
        s["n2"]["preTasks"][0].setInput(s["n1"]["task"])

        s["n3"] = GafferDispatchTest.LoggingTaskNode()
        s["n3"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n3"]["dispatcher"]["batchSize"].setValue(50)
        s["n3"]["preTasks"][0].setInput(s["n1"]["task"])

        s["n4"] = GafferDispatchTest.LoggingTaskNode()
        s["n4"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n4"]["dispatcher"]["batchSize"].setValue(15)
        s["n4"]["preTasks"][0].setInput(s["n2"]["task"])

        s["t1"] = GafferDispatch.TaskList()
        s["t1"]["dispatcher"]["batchSize"].setValue(1)
        s["t1"]["preTasks"][0].setInput(s["n4"]["task"])
        s["t1"]["preTasks"][1].setInput(s["n3"]["task"])

        s["n5"] = GafferDispatchTest.LoggingTaskNode()
        s["n5"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n5"]["preTasks"][0].setInput(s["t1"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n5"]], dispatcher)

        self.assertEqual(len(jobs), 5)
        for j in jobs:

            if j.getJobProperties()["Name"] == "n1":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 50)

            elif j.getJobProperties()["Name"] == "n2":
                self.assertEqual(len(j.getDependencies()), 50)
                self.assertEqual(len(j.getTasks()), 4)
                self.assertEqual(
                    j.getDependencyType(),
                    GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.FrameToFrame
                )
            elif j.getJobProperties()["Name"] == "n3":
                self.assertEqual(len(j.getDependencies()), 50)
                self.assertEqual(len(j.getTasks()), 1)
                self.assertEqual(
                    j.getDependencyType(),
                    GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.FrameToFrame
                )
            elif j.getJobProperties()["Name"] == "n4":
                self.assertEqual(len(j.getDependencies()), 4)
                self.assertEqual(len(j.getTasks()), 4)
                self.assertEqual(
                    j.getDependencyType(),
                    GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.FrameToFrame
                )

            elif j.getJobProperties()["Name"] == "n5":
                # self.__debugPrintDependencies(j.getDependencies())
                self.assertEqual(len(j.getDependencies()), 100)
                self.assertEqual(len(j.getTasks()), 50)
                self.assertEqual(
                    j.getDependencyType(),
                    GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.FrameToFrame
                )

    def testFrameMask(self):
        #   n1
        #   |
        #   m1
        #   |
        #   n2
        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n1"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n1"]["dispatcher"]["batchSize"].setValue(1)

        s["m1"] = GafferDispatch.FrameMask()
        s["m1"]["mask"].setValue("16-20")
        s["m1"]["dispatcher"]["batchSize"].setValue(1)
        s["m1"]["preTasks"][0].setInput(s["n1"]["task"])

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n2"]["dispatcher"]["batchSize"].setValue(1)
        s["n2"]["preTasks"][0].setInput(s["m1"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n2"]], dispatcher)

        self.assertEqual(len(jobs), 2)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n2":
                self.assertEqual(len(j.getDependencies()), 5)
                self.assertEqual(len(j.getTasks()), 50)
                self.assertEqual(
                    j.getDependencyType(),
                    GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.FrameToFrame
                )
            elif j.getJobProperties()["Name"] == "n1":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 5)

    def testOffsetFrameDependency(self):
        #   n1
        #   |
        #   c1 ---- e1
        #   |
        #   n2
        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n1"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n1"]["dispatcher"]["batchSize"].setValue(1)

        s["c1"] = GafferDispatch.TaskContextVariables()
        s["c1"]["variables"].addChild(
            Gaffer.CompoundDataPlug.MemberPlug(
                "member1", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic,
            )
        )
        s["c1"]["variables"]["member1"].addChild(
            Gaffer.StringPlug(
                "name",
                defaultValue='',
                flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic,
            )
        )
        s["c1"]["variables"]["member1"].addChild(
            Gaffer.FloatPlug(
                "value",
                defaultValue=0.0,
                flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic,
            )
        )
        s["c1"]["variables"]["member1"].addChild(
            Gaffer.BoolPlug(
                "enabled",
                defaultValue=True,
                flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic,
            )
        )
        s["c1"]["variables"]["member1"]["name"].setValue('frame')
        s["c1"]["dispatcher"]["batchSize"].setValue(1)
        s["c1"]["preTasks"][0].setInput(s["n1"]["task"])

        s["e"] = Gaffer.Expression()
        s["e"].setExpression(
            'parent["c1"]["variables"]["member1"]["value"] = context.getFrame() + 100',
            "python"
        )

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n2"]["dispatcher"]["batchSize"].setValue(1)
        s["n2"]["preTasks"][0].setInput(s["c1"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n2"]], dispatcher)

        self.assertEqual(len(jobs), 2)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n2":
                self.assertEqual(len(j.getDependencies()), 50)
                self.assertEqual(len(j.getTasks()), 50)
                self.assertEqual(
                    j.getDependencyType(),
                    GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.FrameToFrame
                )
                self.assertEqual(j._frameDependencyOffsetStart, 100)
                self.assertEqual(j._frameDependencyOffsetEnd, 100)
            elif j.getJobProperties()["Name"] == "n1":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 50)

    def testScriptFrameDependency(self):
        #   n1
        #   |
        #   c1 ---- e1
        #   |
        #   n2
        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n1"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n1"]["dispatcher"]["batchSize"].setValue(1)

        s["c1"] = GafferDispatch.TaskContextVariables()
        s["c1"]["variables"].addChild(
            Gaffer.CompoundDataPlug.MemberPlug(
                "member1",
                flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic,
            )
        )
        s["c1"]["variables"]["member1"].addChild(
            Gaffer.StringPlug(
                "name", defaultValue='',
                flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic,
            )
        )
        s["c1"]["variables"]["member1"].addChild(
            Gaffer.FloatPlug(
                "value",
                defaultValue=0.0,
                flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic,
            )
        )
        s["c1"]["variables"]["member1"].addChild(
            Gaffer.BoolPlug(
                "enabled",
                defaultValue=True,
                flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic,
            )
        )
        s["c1"]["variables"]["member1"]["name"].setValue('frame')
        s["c1"]["dispatcher"]["batchSize"].setValue(1)
        s["c1"]["preTasks"][0].setInput(s["n1"]["task"])

        s["e"] = Gaffer.Expression()
        s["e"].setExpression(
            'import random\nrandom.seed=(context.getFrame())\nparent["c1"]["variables"]["member1"]'
            '["value"] = random.randint(1,100000)',
            "python"
        )

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n2"]["dispatcher"]["batchSize"].setValue(1)
        s["n2"]["preTasks"][0].setInput(s["c1"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch(
            'GafferDeadline.DeadlineTools.submitJob',
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n2"]], dispatcher)

        self.assertEqual(len(jobs), 2)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n2":
                self.assertEqual(len(j.getDependencies()), 50)
                self.assertEqual(len(j.getTasks()), 50)
                self.assertEqual(
                    j.getDependencyType(),
                    GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.Scripted
                )
            elif j.getJobProperties()["Name"] == "n1":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 50)

    def testControlTaskDependency(self):
        #   n1 (LoggingTaskNode)
        #   |
        #   m1 (FrameMask)
        #   |
        #   w1 (Wedge)

        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n1"]["frame"] = Gaffer.StringPlug(
            defaultValue="${wedge:value}.${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )

        s["m1"] = GafferDispatch.FrameMask()
        s["m1"]["mask"].setValue("1-10")
        s["m1"]["preTasks"][0].setInput(s["n1"]["task"])

        s["w1"] = GafferDispatch.Wedge()
        s["w1"]["mode"].setValue(int(GafferDispatch.Wedge.Mode.StringList))
        s["w1"]["strings"].setValue(IECore.StringVectorData(["a", "b", "c"]))
        s["w1"]["preTasks"][0].setInput(s["m1"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["w1"]], dispatcher)

        self.assertEqual(len(jobs), 3)  # A LoggingTaskNode per Wedge entry
        for j in jobs:
            if j.getJobProperties()["Name"] == "n1":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 10)

    def testNoOpBatch(self):
        #    n1
        #    |
        #    t1
        #    |
        #    n2
        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n1"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )

        s["t1"] = GafferDispatch.TaskList()
        s["t1"]["dispatcher"]["batchSize"].setValue(10)
        s["t1"]["preTasks"][0].setInput(s["n1"]["task"])

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(
            defaultValue="${frame}",
            flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
        )
        s["n2"]["preTasks"][0].setInput(s["t1"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ), IECore.CapturingMessageHandler() as mh:
            dispatcher.dispatch([s["n2"]])
            warnings = [i for i in mh.messages if i.level == IECore.Msg.Level.Warning]
            self.assertEqual(len(warnings), 50)
            for warning in warnings:
                self.assertEqual(
                    warning.message,
                    "No-Op node t1 has a batch size greater than 1 which will be ignored."
                )

    def testContext(self):
        s = Gaffer.ScriptNode()

        s["n"] = GafferDispatchTest.LoggingTaskNode()
        s["n"]["dispatcher"]["deadline"]["comment"].setValue("${wedge:value}")
        s["n"]["dispatcher"]["deadline"]["deadlineSettings"].addChild(
            Gaffer.NameValuePlug(
                "Name",
                IECore.StringData("${wedge:value}"),
                True,
                "member1",
                Gaffer.Plug.Direction.In,
                Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
            )
        )
        s["n"]["dispatcher"]["deadline"]["environmentVariables"].addChild(
            Gaffer.NameValuePlug(
                "Index",
                IECore.StringData("${wedge:index}"),
                True,
                "member2",
                Gaffer.Plug.Direction.In,
                Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
            )
        )

        s["w"] = GafferDispatch.Wedge()
        s["w"]["preTasks"][0].setInput(s["n"]["task"])
        s["w"]["mode"].setValue(GafferDispatch.Wedge.Mode.StringList)
        s["w"]["strings"].setValue(IECore.StringVectorData(["I'mAName"]))

        s["d"] = GafferDeadline.DeadlineDispatcher()
        s["d"]["preTasks"][0].setInput(s["w"]["task"])

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["w"]])

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].getJobProperties()["Name"], "I'mAName")
        self.assertEqual(jobs[0].getEnvironmentVariables()["Index"], "0")
        self.assertEqual(jobs[0].getJobProperties()["Comment"], "I'mAName")

    def testExtra(self):
        s = Gaffer.ScriptNode()

        s["n"] = GafferDispatchTest.LoggingTaskNode()
        s["n"]["dispatcher"]["deadline"]["deadlineSettings"].addChild(
            Gaffer.NameValuePlug(
                "Name",
                IECore.StringData("NotLittleDebbie"),
                True,
                "member1",
                Gaffer.Plug.Direction.In,
                Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
            )
        )
        s["n"]["dispatcher"]["deadline"]["environmentVariables"].addChild(
            Gaffer.NameValuePlug(
                "Index",
                IECore.StringData("1000"),
                True,
                "member2",
                Gaffer.Plug.Direction.In,
                Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic
            )
        )
        s["n"]["dispatcher"]["deadline"]["extraDeadlineSettings"].setValue(
            IECore.CompoundObject(
                {
                    "Name": IECore.StringData("LittleDebbie"),
                    "MachineName": IECore.StringData("Francis"),
                }
            )
        )
        s["n"]["dispatcher"]["deadline"]["extraEnvironmentVariables"].setValue(
            IECore.CompoundObject(
                {
                    "Index": IECore.IntData(0),
                    "ARNOLD_ROOT": IECore.StringData("/arnoldRoot"),
                }
            )
        )

        s["d"] = GafferDeadline.DeadlineDispatcher()
        s["d"]["preTasks"][0].setInput(s["n"]["task"])

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n"]])

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].getJobProperties()["Name"], "LittleDebbie")
        self.assertEqual(jobs[0].getJobProperties()["MachineName"], "Francis")
        self.assertEqual(jobs[0].getEnvironmentVariables()["Index"], "0")
        self.assertEqual(jobs[0].getEnvironmentVariables()["ARNOLD_ROOT"], "/arnoldRoot")

    def testName(self):
        s = Gaffer.ScriptNode()

        s["n"] = GafferDispatchTest.LoggingTaskNode()

        s["d"] = GafferDeadline.DeadlineDispatcher()
        s["d"]["jobsDirectory"].setValue(self.temporaryDirectory() / "testJobDirectory")
        s["d"]["preTasks"][0].setInput(s["n"]["task"])

        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n"]], dispatcher=s["d"])

        self.assertEqual(jobs[0].getJobProperties()["Name"], "n")

        s["d"]["jobName"].setValue("Harvey")
        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n"]], dispatcher=s["d"])

        self.assertEqual(jobs[0].getJobProperties()["Name"], "Harvey.n")

        s["n"]["dispatcher"]["deadline"]["extraDeadlineSettings"].setValue(
            IECore.CompoundObject(
                {
                    "Name": IECore.StringData("LittleDebbie"),
                }
            )
        )
        with mock.patch(
            "GafferDeadline.DeadlineTools.submitJob",
            return_value=("testID", "testMessage")
        ):
            jobs = self.__job([s["n"]], dispatcher=s["d"])

        self.assertEqual(jobs[0].getJobProperties()["Name"], "LittleDebbie")


if __name__ == "__main__":
    unittest.main()
