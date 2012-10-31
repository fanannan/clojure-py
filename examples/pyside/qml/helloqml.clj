; sample taken from http://doc.qt.nokia.com/4.7-snapshot/qml-tutorial2.html
(ns exampes.pyside.qml.helloqml 
    (:require [PySide.QtGui :as gui]
              [PySide.QtDeclarative :as decl]
              [PySide.QtCore :as core]))


(let [app (gui/QApplication sys/argv)
      view (decl/QDeclarativeView)]
      (.setWindowTitle view "PySide/QML/clojure-py")
      (.setSource view (.fromLocalFile core/QUrl "helloqml.qml"))
      (.setResizeMode view (.-SizeRootObjectToView decl/QDeclarativeView))
      (.show view)
      (sys/exit (.exec_ app)))
