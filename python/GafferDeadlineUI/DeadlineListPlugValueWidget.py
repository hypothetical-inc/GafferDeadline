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
import IECore

from GafferDeadline import DeadlineTools


class DeadlineListPlugValueWidget(GafferUI.PlugValueWidget):

    def __init__(self, plug, listString="", **kw):
        assert type(listString) == str

        self.__row = GafferUI.ListContainer(
            GafferUI.ListContainer.Orientation.Horizontal,
            spacing=4
        )
        GafferUI.PlugValueWidget.__init__(self, self.__row, plug, **kw)

        self.__listString = listString

        listWidget = GafferUI.TextWidget(self.__listString)
        self._addPopupMenu(listWidget)
        self.__row.append(listWidget)

        button = GafferUI.Button("...", hasFrame=True)
        self.__buttonClickedConnection = button.clickedSignal().connect(
            Gaffer.WeakMethod(self.__buttonClicked),
            scoped=True
        )
        self.__row.append(button)

        self.__editingFinishedConnection = listWidget.editingFinishedSignal().connect(
            Gaffer.WeakMethod(self.__setPlugValue),
            scoped=True
        )

        self._updateFromPlug()

    def listWidget(self):
        return self.__row[0]

    def __buttonClicked(self, widget):
        # Get info from the farm.
        # Keep this in the button code to prevent it from being called repeatedly by Gaffer with
        # every node with a Deadline submitter attached
        multiSelect = Gaffer.Metadata.value(
            self.getPlug(),
            "deadlineListPlugValueWidget:multiSelect"
        )
        if Gaffer.Metadata.value(self.getPlug(), "deadlineListPlugValueWidget:type") == "pools":
            deadlineListString = ",".join(DeadlineTools.getPools())
            dialogTitle = "Select Pools"
        elif Gaffer.Metadata.value(self.getPlug(), "deadlineListPlugValueWidget:type") == "groups":
            deadlineListString = ",".join(DeadlineTools.getGroups())
            dialogTitle = "Select Groups"
        elif Gaffer.Metadata.value(self.getPlug(), "deadlineListPlugValueWidget:type") == "slaves":
            deadlineListString = ",".join(DeadlineTools.getMachineList())
            dialogTitle = "Select Slaves"
        elif Gaffer.Metadata.value(self.getPlug(), "deadlineListPlugValueWidget:type") == "limits":
            deadlineListString = ",".join(DeadlineTools.getLimitGroups())
            dialogTitle = "Select Limits"

        optionListString = deadlineListString.split(",")
        selectionString = self.getPlug().getValue().split(",")
        dialogue = GafferDeadlineUI.ListSelectionDialog(
            optionListString,
            selectionString,
            dialogTitle,
            allowMultipleSelection=multiSelect
        )
        listString = dialogue.waitForSelection(parentWindow=self.ancestor(GafferUI.Window))

        if listString is not None:
            self.__listString = listString
            self.__setPlugValue()

    def _updateFromPlug(self):
        with self.getContext():
            with IECore.IgnoredExceptions(ValueError):
                self.__listString = self.getPlug().getValue()
                assert type(self.__listString) == str

        self.listWidget().setEditable(self._editable())
        self.__row[1].setEnabled(self._editable())  # button
        self.listWidget().setText(self.__listString)

    def _setPlugFromString(self, listString):
        self.getPlug().setValue(listString)
        self._updateFromPlug()

    def setHighlighted(self, highlighted):
        GafferUI.PlugValueWidget.setHighlighted(self, highlighted)

    def setReadOnly(self, readOnly):
        if readOnly == self.getReadOnly():
            return

        GafferUI.PlugValueWidget.setReadOnly(self, readOnly)

    def __setPlugValue(self, *args):
        if not self._editable():
            return

        with Gaffer.UndoScope(self.getPlug().ancestor(Gaffer.ScriptNode)):
            if args:
                self._setPlugFromString(args[0].getText())
            else:
                self._setPlugFromString(self.__listString)

        # now we've transferred the text changes to the global undo queue, we remove them
        # from the widget's private text editing undo queue. it will then ignore undo shortcuts,
        # allowing them to fall through to the global undo shortcut.
        self.listWidget().clearUndo()
