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

import Gaffer
import GafferUI
import GafferDeadlineUI


class ListSelectionDialog(GafferUI.Dialogue):
    def __init__(
        self,
        masterList,
        selectionList,
        title="Choose Wisely",
        cancelLabel="Cancel",
        confirmLabel="OK",
        allowMultipleSelection=False, **kw
    ):
        GafferUI.Dialogue.__init__(self, title, **kw)
        self.__masterList = masterList
        self.__selectionList = [i for i in selectionList if i in masterList]

        self.__masterListWidget = GafferDeadlineUI.ListWidget(
            allowMultipleSelection=allowMultipleSelection
        )
        for i in self.__masterList:
            self.__masterListWidget.addItem(i)
        self.__masterListWidget.setSelectedStrings(self.__selectionList)

        self._setWidget(self.__masterListWidget)

        self.__cancelButton = self._addButton(cancelLabel)
        self.__cancelButtonConnection = self.__cancelButton.clickedSignal().connect(
            Gaffer.WeakMethod(self.__buttonClicked),
            scoped=True
        )
        self.__confirmButton = self._addButton(confirmLabel)
        self.__confirmButtonConnection = self.__confirmButton.clickedSignal().connect(
            Gaffer.WeakMethod(self.__buttonClicked),
            scoped=True
        )

    def waitForSelection(self, **kw):
        button = self.waitForButton(**kw)

        if button is self.__confirmButton:
            return self.__result()

        return None

    def __result(self):
        stringList = self.__masterListWidget.getSelectedStrings()
        return ",".join(stringList)

    def __buttonClicked(self, button):
        if button is self.__confirmButton:
            pass
