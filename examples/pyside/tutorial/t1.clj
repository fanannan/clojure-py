(ns examples.pyside.tutorial.t1
    (:require [PySide.QtGui :as gui]))




(let [app (gui/QApplication  sys/argv)
      hello (gui/QPushButton "Hello world!")]
      (.resize hello 100 30)
      (.show hello)
      (sys/exit (.exec_ app)))
      

