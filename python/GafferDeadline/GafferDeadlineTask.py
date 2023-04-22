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

import IECore

import Gaffer
import GafferDispatch


class GafferDeadlineTask(object):
    """ Mimic the Deadline representation of a task:
    - tasks are a sequential range of frames indicated by the start frame and end frame
    - tasks can only be associated with one job and therefore one batch / Gaffer Task Node
    """
    def __init__(self, gafferBatch, taskNumber, startFrame=None, endFrame=None):
        self._startFrame = None
        self._endFrame = None

        self.setGafferBatch(gafferBatch)
        self.setStartFrame(startFrame)
        self.setEndFrame(endFrame)
        self.setTaskNumber(taskNumber)

        if self.getStartFrame() is None and self.getGafferBatch() is not None and len(
            self.getGafferBatch().frames()
        ) > 0:
            self.setStartFrame(self.getGafferBatch.frames()[0])
        if self.getEndFrame() is None and self.getGafferBatch() is not None and len(
            self.getGafferBatch().frames()
        ) > 0:
            self.setEndFrame(self.getGafferBatch.frames()[-1])

    def __hash__(self):
        h = IECore.MurmurHash()

        # hash the batch without the frame number as Gaffer does in Dispatcher::Batcher::batchHash
        b = self.getGafferBatch()
        h.append(hash(b.plug()))
        c = Gaffer.Context(b.context())
        c.remove("frame")
        h.append(c.hash())

        h.append(self.getStartFrame() if self.getStartFrame() is not None else 1)
        h.append(self.getEndFrame() if self.getEndFrame() is not None else 1)
        h.append(self.getTaskNumber())

        return hash(h)

    def setTaskNumber(self, taskNumber):
        assert type(taskNumber) == int
        self._taskNumber = taskNumber

    def getTaskNumber(self):
        return self._taskNumber

    def setGafferBatch(self, gafferBatch):
        assert gafferBatch is None or type(gafferBatch) == GafferDispatch.Dispatcher._TaskBatch
        self._gafferBatch = gafferBatch

    def getGafferBatch(self):
        return self._gafferBatch

    def setFrameRange(self, startFrame, endFrame):
        if endFrame < startFrame:
            raise ValueError("End frame must be greater than start frame.")
        if int(startFrame) != startFrame or int(endFrame) != endFrame:
            raise ValueError("Start and end frames must be integers.")
        self._startFrame = int(startFrame)
        self._endFrame = int(endFrame)

    def setFrameRangeFromList(self, frameList):
        framesSequential = True
        if len(frameList) > 0:
            if int(frameList[0]) != frameList[0]:
                raise ValueError("Frame numbers must be integers.")
            for i in range(1, len(frameList)-1):
                if int(frameList[i]) != frameList[i]:
                    raise ValueError("Frame numbers must be integers.")
                if frameList[i] - frameList[i-1] != 1:
                    framesSequential = False

            if not framesSequential:
                raise ValueError("Frame list must be sequential.")
            self._startFrame = int(frameList[0])
            self._endFrame = int(frameList[len(frameList) - 1])
        else:
            self.setStartFrame(None)
            self.setEndFrame(None)

    def setStartFrame(self, startFrame):
        if (
            self._endFrame is not None and
            startFrame is not None and
            startFrame > self._endFrame
        ):
            raise ValueError("Start frame must be less than end frame.")
        if startFrame is not None:
            if int(startFrame) != startFrame:
                raise ValueError("Frame numbers must be integers.")
            self._startFrame = int(startFrame)
        else:
            self._startFrame = None

    def getStartFrame(self):
        return self._startFrame

    def setEndFrame(self, endFrame):
        if (
            self._startFrame is not None and
            endFrame is not None and
            endFrame < self._startFrame
        ):
            raise ValueError("End frame must be greater than start frame.")
        if endFrame is not None:
            if int(endFrame) != endFrame:
                raise ValueError("Frame numbers must be integers.")
            self._endFrame = int(endFrame)
        else:
            self._endFrame = None

    def getEndFrame(self):
        return self._endFrame
