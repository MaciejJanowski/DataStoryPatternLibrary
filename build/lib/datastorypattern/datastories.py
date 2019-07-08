from SPARQLWrapper import SPARQLWrapper, SPARQLWrapper2, JSON, JSONLD, CSV, TSV, N3, RDF, RDFXML, TURTLE
import itertools 
import sparql_dataframe
import pandas as pd
import json
import numpy as np
from scipy import stats

class DataStoryPatterns():

    def __init__(self,sparqlEndpoint,metadatafile):
        self.sparqlEndpoint=sparqlEndpoint
        metadatafile=open(metadatafile).read()
        self.metaDataDict=json.loads(metadatafile)

    def retrieveData(self,cube,dims,meas,hierdims=[]):
        """
        retrieveData - SPARQL query builder to retrieve data from SPARQL endpoint.
   
        ...

        Attributes
        --------------

        cube: str
            Cube to retrieve data
        dims: list[str]
            list of Strings (dimension names) to retrieve
        meas: list[str]
            list of measures to retrieve
        hierdims: dict{hierdim:{"selected_level":[value]}}
            hierarchical dimension (if provided) to retrieve data from specific
            hierarchical level
        
        ...
        Output
        ------------
        Pandas Dataframe 

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

    def MeasurentCounting(self,cube=[],dims=[],meas=[],hierdims=[],count_type="raw",df=pd.DataFrame() ): 
        """
        MeasurementCounting - arithemtical operators applied to whole dataset
        ...
        Attributes
        ------------
        cube: str
            Cube to retrieve data
        dims: list[str]
            list of Strings (dimension names) to retrieve
        meas: list[str]
            list of measures to retrieve
        hierdims: dict{hierdim:{"selected_level":[value]}}
            hierarchical dimension (if provided) to retrieve data from specific
            hierarchical level
        count_type: str
            type of count operator to perform on data
        df: dataframe
            if data is not already retrieved, dataframe can be specified 
        ...
        Output
        --------
        Based on count_type value:
            raw-> data without any analysis performed
            sum-> sum across all numeric columns
            mean-> arithmetic mean across all numeric columns
            min-> minium values from all numeric columns
            max-> maximum values from all numeric columns
            count-> amount of records within data

        """
        if(df.empty):
            dataframe=self.retrieveData(cube,dims,meas,hierdims)
        else:
            dataframe=df
        if(count_type=="raw"):
            return dataframe
        elif(count_type=="sum"):
            return dataframe.sum(axis=1, skipna=True)
        elif(count_type=="mean"):
            return dataframe.mean(numeric_only=True)
        elif(count_type=="min"):
            return dataframe.min(numeric_only=True)
        elif(count_type=="max"):
            return dataframe.max(numeric_only=True)
        elif(count_type=="count"):
            return dataframe.count()

    def LeagueTable(self,cube=[],dims=[],meas=[],hierdims=[], columns_to_order="", order_type="asc", number_of_records=20,df=pd.DataFrame()):
        """
        LeagueTable - sorting and extraction specific amount of records
        ...
        Attributes
        -------------
        cube: str
            Cube to retrieve data
        dims: list[str]
            list of Strings (dimension names) to retrieve
        meas: list[str]
            list of measures to retrieve
        hierdims: dict{hierdim:{"selected_level":[value]}}
            hierarchical dimension (if provided) to retrieve data from specific
            hierarchical level
        columns_to_order: list[str]
            columns within data to sort by
        order_type: str
            type of order to apply (asc/desc)
        number_of_records: integer
            amount of records to return
        df: dataframe
            if data is already retrieved from SPARQL endpoint, dataframe itself can
            be provided
        ...
        Output
        ------------
        Depending on sort_type
            asc-> ascending order based on columns provided in columns_to_order
            desc-> descending order based on columns provided in columns_to_order
            Amount of records returned will be equal to number_of_records
        """
        if(df.empty):
            dataframe=self.retrieveData(cube,dims,meas,hierdims)
        else:
            dataframe=df
        if(order_type=="asc"):
            return dataframe.sort_values(by=columns_to_order,ascending=True).head(number_of_records)
        elif(order_type=="desc"):
            return dataframe.sort_values(by=columns_to_order, ascending=False).head(number_of_records)

    



