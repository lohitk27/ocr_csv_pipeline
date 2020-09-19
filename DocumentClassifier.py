#!/usr/bin/env python3
# -*- coding: utf-8 -*-

class DocumentClassifier(object):
    
    def __init__(self):
        pass
    
    def GetPipeline(self, docName):
        if docName.split(".")[-1] == "pdf":
            return 3
        else:
            prefix = docName.split("_")[0]
            if prefix == "L":
                return 1
            else:
                return 2
                
    
                