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


class GafferDeadlineTaskTest(GafferTest.TestCase):
    def testSetFrameRange(self):
        self.assertRaises(
            ValueError,
            GafferDeadline.GafferDeadlineTask, None, 1, startFrame=1, endFrame=0
        )
        dt = GafferDeadline.GafferDeadlineTask(None, 1, startFrame=0, endFrame=1)
        self.assertRaises(ValueError, dt.setFrameRange, 1, 0)
        self.assertRaises(ValueError, dt.setFrameRange, 0.1, 100)
        self.assertRaises(ValueError, dt.setFrameRange, 0, 100.1)
        dt.setFrameRange(0, 10)
        self.assertEqual(dt.getStartFrame(), 0)
        self.assertEqual(dt.getEndFrame(), 10)
        dt.setFrameRange(55.0, 75.0)
        self.assertEqual(dt.getStartFrame(), 55)
        self.assertEqual(dt.getEndFrame(), 75)
        self.assertEqual(type(dt.getStartFrame()), int)
        self.assertEqual(type(dt.getEndFrame()), int)

    def testSetFrameRangeFromList(self):
        dt = GafferDeadline.GafferDeadlineTask(None, 1)
        self.assertRaises(ValueError, dt.setFrameRangeFromList, [0, 4, 9])
        self.assertRaises(ValueError, dt.setFrameRangeFromList, [10, 9, 8])
        self.assertRaises(ValueError, dt.setFrameRangeFromList, [1.1, 2.0, 3])
        self.assertRaises(ValueError, dt.setFrameRangeFromList, [1, 2.2, 3])
        dt.setFrameRangeFromList([1, 2, 3, 4])
        self.assertEqual(dt.getStartFrame(), 1)
        self.assertEqual(dt.getEndFrame(), 4)
        dt.setFrameRangeFromList([12.0, 13, 14.0])
        self.assertEqual(dt.getStartFrame(), 12)
        self.assertEqual(dt.getEndFrame(), 14)
        self.assertEqual(type(dt.getStartFrame()), int)
        self.assertEqual(type(dt.getEndFrame()), int)


if __name__ == "__main__":
    unittest.main()
