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

import warnings
import string

import IECore

import Gaffer
import GafferUI
import GafferUI._StyleSheet


import Qt
from Qt import QtCore
from Qt import QtGui
from Qt import QtWidgets


class ListWidget(GafferUI.Widget):

    def __init__(
        self,
        allowMultipleSelection=False,
        **kw
    ):

        GafferUI.Widget.__init__(self, QtWidgets.QListWidget(), **kw)

        self._qtWidget().setAlternatingRowColors(True)
        self._qtWidget().setUniformItemSizes(True)
        self._qtWidget().setResizeMode(QtWidgets.QListView.Adjust)

        # Match Gaffer's QTreeView style
        listWidgetStyleSheet = string.Template(
            """

            *[gafferClass="GafferDeadlineUI.ListWidget"] {
                border-radius: $widgetCornerRadius;
                background-color: $backgroundRaised;
                border: 1px solid $backgroundHighlight;
                border-right-color: $backgroundLowlight;
                border-bottom-color: $backgroundLowlight;
            }
            *[gafferClass="GafferDeadlineUI.ListWidget"]::item::alternate {
                background-color: $backgroundRaisedAlt;
            }
            [gafferClass="GafferDeadlineUI.ListWidget"]::item:selected {
                background-color: $brightColor;
            }
            """
        ).substitute(GafferUI._StyleSheet.substitutions)
        listWidgetStyleSheet += GafferUI._StyleSheet._styleSheet
        self._qtWidget().setStyleSheet(listWidgetStyleSheet)

        if allowMultipleSelection:
            self._qtWidget().setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)

        self.__selectionChangedSlot = Gaffer.WeakMethod(self.__selectionChanged)
        self._qtWidget().selectionModel().selectionChanged.connect(self.__selectionChangedSlot)
        self.__selectionChangedSignal = GafferUI.WidgetSignal()

    def addItem(self, newString):
        self._qtWidget().addItem(newString)

    def reset(self):
        while self._qtWidget().count() > 0:
            self._qtWidget().takeItem(1)

    # Returns a list of all currently selected strings. Note that a list is returned
    # even when in single selection mode.
    def getSelectedStrings(self):
        return [s.text() for s in self._qtWidget().selectedItems()]

    # Sets the currently selected strings. Strings which are not currently being displayed
    # will be discarded, such that subsequent calls to getSelectedStrings will not include them.
    def setSelectedStrings(self, strings, scrollToFirst=True):
        if self._qtWidget().selectionMode() != QtWidgets.QAbstractItemView.ExtendedSelection:
            assert(len(strings) <= 1)

        selectionModel = self._qtWidget().selectionModel()
        selectionModel.selectionChanged.disconnect(self.__selectionChangedSlot)

        selectionModel.clear()

        for string in strings:
            matchingItems = self._qtWidget().findItems(string, QtCore.Qt.MatchExactly)
            for item in matchingItems:
                item.setSelected(True)

        selectionModel.selectionChanged.connect(self.__selectionChangedSlot)

    def selectionChangedSignal(self):

        return self.__selectionChangedSignal

    def __selectionChanged(self, selected, deselected):

        self.selectionChangedSignal()(self)
        return True
