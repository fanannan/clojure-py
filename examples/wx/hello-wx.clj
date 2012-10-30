(ns hello-wx
  (:require [wx]))

(let [app (wx/App.)
      frame (wx/Frame. nil wx/ID_ANY "hello world!")
      txt (wx/TextCtrl. frame 
                        wx/ID_ANY
                        wx/EmptyString
                        wx/DefaultPosition 
                        [300 200]
                        (or wx/TE_AUTO_URL 
                            wx/TE_CHARWRAP
                            wx/TE_MULTILINE))]
  (.SetValue txt "clojure-py says hello to wx.Python")
  (.Show frame)
  (.MainLoop app))
