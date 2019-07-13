import sparql_dataframe
import pandas as pd
import numpy as np
import datastories.analytical as datastories

class datastory(object):

    def __init__(self, sparqlEndpoint,jsonmetadata):
        self.sparqlEndpoint=sparqlEndpoint
        self.metaDataDict=jsonmetadata


    def getsparql(self):
        return self.sparqlEndpoint

    
    def retrieveData(self,cube,dims,meas,hierdims=[]):
        """
        HELPER FUNCTION NOT STORY PATTERN
        """
        skosInSchemeString="<http://www.w3.org/2004/02/skos/core#inScheme>"
        queryString=""
        queryTemplate="""%s"""
        selectString="SELECT "
        groupByString="GROUP BY "
        whereString="WHERE \n {\n ?s ?p ?o. \n"
        hierarchyLevelString=""
        i=1
        for dimension in dims:
            selectString+=("(str(?dimLabel"+str(i)+") as ?"+self.metaDataDict[cube]["dimensions"][dimension]["dimension_title"]+") ")
            groupByString+=("?dimLabel"+str(i)+" ")
            whereString+="?s <"+self.metaDataDict[cube]["dimensions"][dimension]["dimension_url"]+"> ?dim"+str(i)+" .\n ?dim"+str(i)+" rdfs:label ?dimLabel" + str(i) + ". \n"
            i=i+1
        for hierdimension in hierdims:
            selectString+=("(str(?dimLabel"+str(i)+") as ?"+self.metaDataDict[cube]["hierarchical_dimensions"][hierdimension]["dimension_title"]+") ")
            groupByString+=("?dimLabel"+str(i)+" ")
            if(hierdims[hierdimension]["selected_level"]):
                hierarchyLevelString+="?dim"+str(i)+" "+skosInSchemeString+"  <"+self.metaDataDict[cube]["hierarchical_dimensions"][hierdimension]["dimension_prefix"]+hierdims[hierdimension]["selected_level"] +"> .\n"
            whereString+="?s <"+self.metaDataDict[cube]["hierarchical_dimensions"][hierdimension]["dimension_url"]+"> ?dim"+str(i)+" .\n ?dim"+str(i)+" rdfs:label ?dimLabel" + str(i) + ". \n"
            i=i+1
        i=1
        for measure in meas:
            selectString+=(" (SUM(?measure"+str(i)+") as ?"+self.metaDataDict[cube]["measures"][measure]["measure_title"]+") " )
            whereString+=("?s <"+self.metaDataDict[cube]["measures"][measure]["measure_url"]+"> ?measure"+str(i)+" . \n")
            
        whereString+=hierarchyLevelString+"} \n"
        queryString=selectString+whereString+groupByString
        queryTemplate='''%s '''
        sparqldata=sparql_dataframe.get(self.sparqlEndpoint,queryTemplate%(queryString))

        return sparqldata

    

class MeasurementCounting(datastory):
    def analyse(self):
        print (self.getsparql)



data=datastory(sparqlEndpoint="123", jsonmetadata=12313)

print(MeasurementCounting(sparqlEndpoint="123", jsonmetadata=12313).getsparql())

