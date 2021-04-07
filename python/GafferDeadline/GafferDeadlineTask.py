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

import GafferDispatch


class GafferDeadlineTask(object):
    """ Mimic the Deadline representation of a task:
    - tasks are a sequential range of frames indicated by the start frame and end frame
    - tasks can only be associated with one job and therefore one batch / Gaffer Task Node
    """
    def __init__(self, gaffer_batch, task_number, start_frame=None, end_frame=None):
        self._start_frame = None
        self._end_frame = None

        self.setGafferBatch(gaffer_batch)
        self.setStartFrame(start_frame)
        self.setEndFrame(end_frame)
        self.setTaskNumber(task_number)

        if self._start_frame is None and self.getGafferBatch() is not None and len(
            self.getGafferBatch().frames()
        ) > 0:
            self.setStartFrame(gaffer_batch.frames()[0])
        if self._end_frame is None and self.getGafferBatch() is not None and len(
            self.getGafferBatch().frames()
        ) > 0:
            self.setEndFrame(gaffer_batch.frames()[len(gaffer_batch.frames()) - 1])

    def setTaskNumber(self, task_number):
        assert(type(task_number) == int)
        self._task_number = task_number

    def getTaskNumber(self):
        return self._task_number

    def setGafferBatch(self, gaffer_batch):
        assert(gaffer_batch is None or type(gaffer_batch) == GafferDispatch.Dispatcher._TaskBatch)
        self._gaffer_batch = gaffer_batch

    def getGafferBatch(self):
        return self._gaffer_batch

    def setFrameRange(self, start_frame, end_frame):
        if end_frame < start_frame:
            raise ValueError("End frame must be greater than start frame.")
        if int(start_frame) != start_frame or int(end_frame) != end_frame:
            raise ValueError("Start and end frames must be integers.")
        self._start_frame = int(start_frame)
        self._end_frame = int(end_frame)

    def setFrameRangeFromList(self, frame_list):
        frames_sequential = True
        if len(frame_list) > 0:
            if int(frame_list[0]) != frame_list[0]:
                raise ValueError("Frame numbers must be integers.")
            for i in range(1, len(frame_list)-1):
                if int(frame_list[i]) != frame_list[i]:
                    raise ValueError("Frame numbers must be integers.")
                if frame_list[i] - frame_list[i-1] != 1:
                    frames_sequential = False

            if not frames_sequential:
                raise ValueError("Frame list must be sequential.")
            self._start_frame = int(frame_list[0])
            self._end_frame = int(frame_list[len(frame_list) - 1])
        else:
            self.setStartFrame(None)
            self.setEndFrame(None)

    def setStartFrame(self, start_frame):
        if (
            self._end_frame is not None and
            start_frame is not None and
            start_frame > self._end_frame
        ):
            raise ValueError("Start frame must be less than end frame.")
        if start_frame is not None:
            if int(start_frame) != start_frame:
                raise ValueError("Frame numbers must be integers.")
            self._start_frame = int(start_frame)
        else:
            self._start_frame = None

    def getStartFrame(self):
        return self._start_frame

    def setEndFrame(self, end_frame):
        if (
            self._start_frame is not None and
            end_frame is not None and
            end_frame < self._start_frame
        ):
            raise ValueError("End frame must be greater than start frame.")
        if end_frame is not None:
            if int(end_frame) != end_frame:
                raise ValueError("Frame numbers must be integers.")
            self._end_frame = int(end_frame)
        else:
            self._end_frame = None

    def getEndFrame(self):
        return self._end_frame
