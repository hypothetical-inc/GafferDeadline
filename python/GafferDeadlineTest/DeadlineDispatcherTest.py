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
import mock

import Gaffer
import GafferTest
import GafferDispatch
import GafferDispatchTest
import GafferDeadline


class DeadlineDispatcherTest(GafferTest.TestCase):
    def __dispatcher(self):
        dispatcher = GafferDeadline.DeadlineDispatcher()
        dispatcher["jobsDirectory"].setValue(os.path.join(self.temporaryDirectory(), "testJobDirectory").replace("\\", "\\\\"))

        return dispatcher

    def __job(self, nodes, dispatcher=None):
        jobs = []

        def f(dispatcher, job):
            jobs.append(job)

        c = GafferDeadline.DeadlineDispatcher.preSpoolSignal().connect(f)

        if dispatcher is None:
            dispatcher = self.__dispatcher()

        dispatcher.dispatch(nodes)

        return jobs

    def testPreSpoolSignal(self):
        s = Gaffer.ScriptNode()
        s["n"] = GafferDispatchTest.LoggingTaskNode()

        spooled = []

        def f(dispatcher, job):
            spooled.append((dispatcher, job))

        c = GafferDeadline.DeadlineDispatcher.preSpoolSignal().connect(f)

        dispatcher = self.__dispatcher()
        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
            dispatcher.dispatch([s["n"]])

        self.assertEqual(len(spooled), 1)
        self.assertTrue(spooled[0][0] is dispatcher)

    def testJobScript(self):
        s = Gaffer.ScriptNode()
        s["n"] = GafferDispatchTest.LoggingTaskNode()

        dispatcher = self.__dispatcher()
        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
            dispatcher.dispatch([s["n"]])

        self.assertTrue(os.path.isfile(os.path.join(dispatcher.jobDirectory(), "n.job")))
        self.assertTrue(os.path.isfile(os.path.join(dispatcher.jobDirectory(), "n.plugin")))

    def testTaskAttributes(self):
        s = Gaffer.ScriptNode()
        s["n"] = GafferDispatchTest.LoggingTaskNode()
        s["n"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n"]["dispatcher"]["batchSize"].setValue(10)

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-10")

        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
            jobs = self.__job([s["n"]], dispatcher)
        self.assertEqual(len(jobs[0].getParentJobs()), 0)
        self.assertEqual(len(jobs[0].getTasks()), 1)

    def testMultipleBatches(self):
        s = Gaffer.ScriptNode()
        s["n"] = GafferDispatchTest.LoggingTaskNode()
        s["n"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n"]["dispatcher"]["batchSize"].setValue(1)

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-10")

        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
            jobs = self.__job([s["n"]], dispatcher)

        self.assertEqual(len(jobs), 1)
        self.assertEqual(len(jobs[0].getParentJobs()), 0)
        self.assertEqual(len(jobs[0].getTasks()), 10)

    def testSingleBatch(self):
        s = Gaffer.ScriptNode()
        s["n"] = GafferDispatchTest.LoggingTaskNode()
        s["n"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n"]["dispatcher"]["batchSize"].setValue(10)

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-10")

        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
            jobs = self.__job([s["n"]], dispatcher)

        self.assertEqual(len(jobs), 1)
        self.assertEqual(len(jobs[0].getTasks()), 1)

    def testCollapseIdenticalFrames(self):
        s = Gaffer.ScriptNode()
        s["n"] = GafferDispatch.PythonCommand()
        s["n"]["command"] = Gaffer.StringPlug(defaultValue="print(\"Hello\")", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n"]["dispatcher"]["batchSize"].setValue(1)

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-10")

        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
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

        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
            jobs = self.__job([s["n2"], s["n3"]])

        self.assertEqual(len(jobs), 3)
        dependency_count = {"n1":0, "n2": 1, "n3": 0}
        self.assertEqual(len(jobs[0].getParentJobs()), dependency_count[jobs[0].getJobProperties()["Name"]])
        self.assertEqual(len(jobs[1].getParentJobs()), dependency_count[jobs[1].getJobProperties()["Name"]])
        self.assertEqual(len(jobs[2].getParentJobs()), dependency_count[jobs[2].getJobProperties()["Name"]])

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

        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
            jobs = self.__job([s["n2"]])

        self.assertEqual(len(jobs), 4)
        self.assertEqual(len(jobs[-1].getParentJobs()), 2)
        self.assertEqual(len(jobs[-1].getParentJobs()[0].getParentJobs()), 1)
        self.assertEqual(len(jobs[-1].getParentJobs()[0].getParentJobs()[0].getParentJobs()), 0)

    def testNoDependencies(self):
        #   n1
        #   |
        #   n2

        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n1"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n1"]["dispatcher"]["batchSize"].setValue(10)

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n2"]["dispatcher"]["batchSize"].setValue(13)
        s["n2"]["preTasks"][0].setInput(s["n1"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
            jobs = self.__job([s["n2"]], dispatcher)

        self.assertEqual(len(jobs), 2)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n2":
                self.assertEqual(len(j.getDependencies()), 8)
                self.assertEqual(len(j.getTasks()), 4)
            elif j.getJobProperties()["Name"] == "n1":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 5)
                self.assertEqual(j.getDependencyType(), GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.none)

    def testOverrideNone(self):
        #   n1
        #   |
        #   n2

        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n1"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n1"]["dispatcher"]["batchSize"].setValue(10)

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n2"]["dispatcher"]["batchSize"].setValue(13)
        s["n2"]["dispatcher"]["deadline"]["dependencyMode"].setValue("None")
        s["n2"]["preTasks"][0].setInput(s["n1"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
            jobs = self.__job([s["n2"]], dispatcher)

        self.assertEqual(len(jobs), 2)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n2":
                self.assertEqual(len(j.getDependencies()), 8)
                self.assertEqual(len(j.getTasks()), 4)
                self.assertEqual(j.getDependencyType(), GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.none)
            elif j.getJobProperties()["Name"] == "n1":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 5)
                self.assertEqual(j.getDependencyType(), GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.none)

    def testOverrideJob(self):
        #   n1
        #   |
        #   n2

        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n1"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n1"]["dispatcher"]["batchSize"].setValue(10)

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n2"]["dispatcher"]["batchSize"].setValue(13)
        s["n2"]["dispatcher"]["deadline"]["dependencyMode"].setValue("Job")
        s["n2"]["preTasks"][0].setInput(s["n1"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
            jobs = self.__job([s["n2"]], dispatcher)

        self.assertEqual(len(jobs), 2)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n2":
                self.assertEqual(len(j.getDependencies()), 8)
                self.assertEqual(len(j.getTasks()), 4)
                self.assertEqual(j.getDependencyType(), GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.job_to_job)
            elif j.getJobProperties()["Name"] == "n1":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 5)
                self.assertEqual(j.getDependencyType(), GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.none)

    def testOverrideJob(self):
        #   n1
        #   |
        #   n2

        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n1"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n1"]["dispatcher"]["batchSize"].setValue(10)

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n2"]["dispatcher"]["batchSize"].setValue(13)
        s["n2"]["dispatcher"]["deadline"]["dependencyMode"].setValue("Frame")
        s["n2"]["preTasks"][0].setInput(s["n1"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
            jobs = self.__job([s["n2"]], dispatcher)

        self.assertEqual(len(jobs), 2)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n2":
                self.assertEqual(len(j.getDependencies()), 8)
                self.assertEqual(len(j.getTasks()), 4)
                self.assertEqual(j.getDependencyType(), GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.frame_to_frame)
            elif j.getJobProperties()["Name"] == "n1":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 5)
                self.assertEqual(j.getDependencyType(), GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.none)

    def testOverrideScript(self):
        #   n1
        #   |
        #   n2

        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n1"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n1"]["dispatcher"]["batchSize"].setValue(10)

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n2"]["dispatcher"]["batchSize"].setValue(13)
        s["n2"]["dispatcher"]["deadline"]["dependencyMode"].setValue("Script")
        s["n2"]["preTasks"][0].setInput(s["n1"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
            jobs = self.__job([s["n2"]], dispatcher)

        self.assertEqual(len(jobs), 2)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n2":
                self.assertEqual(len(j.getDependencies()), 8)
                self.assertEqual(len(j.getTasks()), 4)
                self.assertEqual(j.getDependencyType(), GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.scripted)
            elif j.getJobProperties()["Name"] == "n1":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 5)
                self.assertEqual(j.getDependencyType(), GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.none)

    def testSequence(self):
        s = Gaffer.ScriptNode()

        s["n"] = GafferDispatch.PythonCommand()
        s["n"]["command"] = Gaffer.StringPlug(defaultValue="print context.getFrame()", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n"]["sequence"].setValue(True)
        s["n"]["dispatcher"]["batchSize"].setValue(1)

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
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
        s["n1"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n1"]["dispatcher"]["batchSize"].setValue(10)

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n2"]["dispatcher"]["batchSize"].setValue(13)

        s["d1"] = Gaffer.Dot()
        s["d1"].setup(s["n2"]["preTasks"][0])
        s["d1"]["in"].setInput(s["n1"]["task"])
        s["n2"]["preTasks"][0].setInput(s["d1"]["out"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
            jobs = self.__job([s["n2"]], dispatcher)

        self.assertEqual(len(jobs), 2)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n2":
                self.assertEqual(len(j.getDependencies()), 8)
                self.assertEqual(len(j.getTasks()), 4)
            elif j.getJobProperties()["Name"] == "n1":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 5)
                self.assertEqual(j.getDependencyType(), GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.none)

    def testDependencies(self):
        #   n1
        #  / \
        # i1 i2
        #  \ /
        #   n2

        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n1"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n1"]["dispatcher"]["batchSize"].setValue(10)

        s["i1"] = GafferDispatchTest.LoggingTaskNode()
        s["i1"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["i1"]["dispatcher"]["batchSize"].setValue(25)
        s["i1"]["preTasks"][0].setInput(s["n1"]["task"])

        s["i2"] = GafferDispatchTest.LoggingTaskNode()
        s["i2"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["i2"]["dispatcher"]["batchSize"].setValue(1)
        s["i2"]["preTasks"][0].setInput(s["n1"]["task"])

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n2"]["dispatcher"]["batchSize"].setValue(13)
        s["n2"]["preTasks"][0].setInput(s["i1"]["task"])
        s["n2"]["preTasks"][1].setInput(s["i2"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
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
        s["n1"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n1"]["dispatcher"]["batchSize"].setValue(10)

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n2"]["dispatcher"]["batchSize"].setValue(25)
        s["n2"]["preTasks"][0].setInput(s["n1"]["task"])

        s["n3"] = GafferDispatchTest.LoggingTaskNode()
        s["n3"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n3"]["dispatcher"]["batchSize"].setValue(1)

        s["n4"] = GafferDispatchTest.LoggingTaskNode()
        s["n4"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n4"]["dispatcher"]["batchSize"].setValue(1)

        s["t1"] = GafferDispatch.TaskList()
        s["t1"]["dispatcher"]["batchSize"].setValue(10)
        s["t1"]["preTasks"][0].setInput(s["n2"]["task"])
        s["t1"]["preTasks"][1].setInput(s["n3"]["task"])

        s["t2"] = GafferDispatch.TaskList()
        s["t2"]["dispatcher"]["batchSize"].setValue(8)
        s["t2"]["preTasks"][0].setInput(s["n4"]["task"])
        s["t2"]["preTasks"][1].setInput(s["t1"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
            jobs = self.__job([s["t2"]], dispatcher)

        self.assertEqual(len(jobs), 6)
        for j in jobs:
            if j.getJobProperties()["Name"] == "t2":
                self.assertEqual(len(j.getDependencies()), 60)
                self.assertEqual(len(j.getTasks()), 7)
            if j.getJobProperties()["Name"] == "t1":
                self.assertEqual(len(j.getDependencies()), 56)
                self.assertEqual(len(j.getTasks()), 5)
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
        s["n1"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n1"]["dispatcher"]["batchSize"].setValue(1)

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n2"]["dispatcher"]["batchSize"].setValue(15)
        s["n2"]["preTasks"][0].setInput(s["n1"]["task"])

        s["n3"] = GafferDispatchTest.LoggingTaskNode()
        s["n3"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n3"]["dispatcher"]["batchSize"].setValue(50)
        s["n3"]["preTasks"][0].setInput(s["n1"]["task"])

        s["n4"] = GafferDispatchTest.LoggingTaskNode()
        s["n4"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n4"]["dispatcher"]["batchSize"].setValue(15)
        s["n4"]["preTasks"][0].setInput(s["n2"]["task"])

        s["t1"] = GafferDispatch.TaskList()
        s["t1"]["dispatcher"]["batchSize"].setValue(10)
        s["t1"]["preTasks"][0].setInput(s["n4"]["task"])
        s["t1"]["preTasks"][1].setInput(s["n3"]["task"])
        

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
            jobs = self.__job([s["t1"]], dispatcher)

        self.assertEqual(len(jobs), 5)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n2":
                self.assertEqual(len(j.getDependencies()), 50)
                self.assertEqual(len(j.getTasks()), 4)
                self.assertEqual(j.getDependencyType(), GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.frame_to_frame)
            elif j.getJobProperties()["Name"] == "n1":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 50)
            elif j.getJobProperties()["Name"] == "n3":
                self.assertEqual(len(j.getDependencies()), 50)
                self.assertEqual(len(j.getTasks()), 1)
                self.assertEqual(j.getDependencyType(), GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.frame_to_frame)
            elif j.getJobProperties()["Name"] == "n4":
                self.assertEqual(len(j.getDependencies()), 4)
                self.assertEqual(len(j.getTasks()), 4)
                self.assertEqual(j.getDependencyType(), GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.frame_to_frame)
            elif j.getJobProperties()["Name"] == "t1":
                self.assertEqual(len(j.getDependencies()), 12)
                self.assertEqual(len(j.getTasks()), 5)
                self.assertEqual(j.getDependencyType(), GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.frame_to_frame)

    def testFrameMask(self):
        #   n1
        #   |
        #   m1
        #   |
        #   n2 
        s = Gaffer.ScriptNode()

        s["n1"] = GafferDispatchTest.LoggingTaskNode()
        s["n1"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n1"]["dispatcher"]["batchSize"].setValue(1)

        s["m1"] = GafferDispatch.FrameMask()
        s["m1"]["mask"].setValue("16-20")
        s["m1"]["dispatcher"]["batchSize"].setValue(1)
        s["m1"]["preTasks"][0].setInput(s["n1"]["task"])

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n2"]["dispatcher"]["batchSize"].setValue(1)
        s["n2"]["preTasks"][0].setInput(s["m1"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
            jobs = self.__job([s["n2"]], dispatcher)

        self.assertEqual(len(jobs), 3)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n2":
                self.assertEqual(len(j.getDependencies()), 50)
                self.assertEqual(len(j.getTasks()), 50)
                self.assertEqual(j.getDependencyType(), GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.job_to_job)
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
        s["n1"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n1"]["dispatcher"]["batchSize"].setValue(1)

        s["c1"] = GafferDispatch.TaskContextVariables()
        s["c1"]["variables"].addChild( Gaffer.CompoundDataPlug.MemberPlug( "member1", flags = Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic, ) )
        s["c1"]["variables"]["member1"].addChild( Gaffer.StringPlug( "name", defaultValue = '', flags = Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic, ) )
        s["c1"]["variables"]["member1"].addChild( Gaffer.FloatPlug( "value", defaultValue = 0.0, flags = Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic, ) )
        s["c1"]["variables"]["member1"].addChild( Gaffer.BoolPlug( "enabled", defaultValue = True, flags = Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic, ) )
        s["c1"]["variables"]["member1"]["name"].setValue( 'frame' )
        s["c1"]["dispatcher"]["batchSize"].setValue(1)
        s["c1"]["preTasks"][0].setInput(s["n1"]["task"])

        s["e"] = Gaffer.Expression()
        s["e"].setExpression( 'parent["c1"]["variables"]["member1"]["value"] = context.getFrame() + 100', "python")

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n2"]["dispatcher"]["batchSize"].setValue(1)
        s["n2"]["preTasks"][0].setInput(s["c1"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
            jobs = self.__job([s["n2"]], dispatcher)

        self.assertEqual(len(jobs), 3)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n2":
                self.assertEqual(len(j.getDependencies()), 50)
                self.assertEqual(len(j.getTasks()), 50)
                self.assertEqual(j.getDependencyType(), GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.frame_to_frame)
                self.assertEqual(j._frame_dependency_offset_start, 100)
                self.assertEqual(j._frame_dependency_offset_end, 100)
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
        s["n1"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n1"]["dispatcher"]["batchSize"].setValue(1)

        s["c1"] = GafferDispatch.TaskContextVariables()
        s["c1"]["variables"].addChild( Gaffer.CompoundDataPlug.MemberPlug( "member1", flags = Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic, ) )
        s["c1"]["variables"]["member1"].addChild( Gaffer.StringPlug( "name", defaultValue = '', flags = Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic, ) )
        s["c1"]["variables"]["member1"].addChild( Gaffer.FloatPlug( "value", defaultValue = 0.0, flags = Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic, ) )
        s["c1"]["variables"]["member1"].addChild( Gaffer.BoolPlug( "enabled", defaultValue = True, flags = Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic, ) )
        s["c1"]["variables"]["member1"]["name"].setValue( 'frame' )
        s["c1"]["dispatcher"]["batchSize"].setValue(1)
        s["c1"]["preTasks"][0].setInput(s["n1"]["task"])

        s["e"] = Gaffer.Expression()
        s["e"].setExpression( 'import random\nrandom.seed=(context.getFrame())\nparent["c1"]["variables"]["member1"]["value"] = random.randint(1,100000)', "python")

        s["n2"] = GafferDispatchTest.LoggingTaskNode()
        s["n2"]["frame"] = Gaffer.StringPlug(defaultValue="${frame}", flags=Gaffer.Plug.Flags.Default | Gaffer.Plug.Flags.Dynamic)
        s["n2"]["dispatcher"]["batchSize"].setValue(1)
        s["n2"]["preTasks"][0].setInput(s["c1"]["task"])

        dispatcher = self.__dispatcher()
        dispatcher["framesMode"].setValue(dispatcher.FramesMode.CustomRange)
        dispatcher["frameRange"].setValue("1-50")

        with mock.patch('GafferDeadline.DeadlineTools.submitJob', return_value=("testID", "testMessage")):
            jobs = self.__job([s["n2"]], dispatcher)

        self.assertEqual(len(jobs), 3)
        for j in jobs:
            if j.getJobProperties()["Name"] == "n2":
                self.assertEqual(len(j.getDependencies()), 50)
                self.assertEqual(len(j.getTasks()), 50)
                self.assertEqual(j.getDependencyType(), GafferDeadline.GafferDeadlineJob.DeadlineDependencyType.scripted)
            elif j.getJobProperties()["Name"] == "n1":
                self.assertEqual(len(j.getDependencies()), 0)
                self.assertEqual(len(j.getTasks()), 50)
        

if __name__ == "__main__":
    unittest.main()
