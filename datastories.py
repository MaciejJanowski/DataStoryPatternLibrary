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

    def InternalComparison(self,cube=[],dims=[],meas=[],hierdims=[],df=pd.DataFrame(), dim_to_compare="",meas_to_compare="",comp_type=""):# TODO
        """
        InternalComparison - comparison of numeric values related to textual values within one column
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
        df: dataframe
            if data is already retrieved from SPARQL endpoint, dataframe itself can
            be provided
        dim_to_compare: str
            dimension, which textual values are bound to be investigated
        meas_to_compare: str
            measure(numeric column), which values related to dim_to_compare 
            will be processed
        comp_type: str
            type of comparison to be performed
        ...
        Output
        -----------
        Independent from comp_type selected, output data will have additional column with numerical
        column processed in specific way.
        Available comparison types (comp_type):
            diffmax->difference with max value related to specific textual value
            diffmean->difference with arithmetic mean related to specific textual value
            diffmin->difference with minimum value related to specific textual value

        """
        
        if(dim_to_compare and meas_to_compare):
            dim_to_compare=dims[0]
            meas_to_compare=meas[0]
        if(df.empty):
            dataframe=self.retrieveData(cube,dims,meas,hierdims)
        else:
            dataframe=df
        if(comp_type=="sum"):
            return dataframe.groupby(dim_to_compare)[meas_to_compare].sum(axis=1)
        elif(comp_type=="diffmax"):
            dataframe["DiffMax"]=dataframe.groupby(dim_to_compare)[meas_to_compare].transform(lambda x: x-x.max())
            return dataframe
        elif(comp_type=="diffmean"):
            dataframe["DiffMean"]=dataframe.groupby(dim_to_compare)[meas_to_compare].transform(lambda x: x-round(x.mean(),2))
            return dataframe
        elif(comp_type=="diffmin"):
            dataframe["DiffMin"]=dataframe.groupby(dim_to_compare)[meas_to_compare].transform(lambda x: x-x.min())
            return dataframe

    def ProfileOutliers(self,cube=[],dims=[],meas=[],hierdims=[],df=pd.DataFrame(), displayType="outliers_only"):
        """
        ProfileOutliers - detection of unusual values within data (anomalies)
        ....
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
        df: dataframe
            if data is already retrieved from SPARQL endpoint, dataframe itself can
            be provided
        displayType: str
            what values are bound to be displayed
        ...
        Output
        -----------
        Based on displayType
            outliers_only->returns rows from dataset where unusual values 
                            were detected
            without_outliers->returns dataset with excluded rows where unusual 
                            values were detecetd
        
        """
        if(df.empty):
            dataframe=self.retrieveData(cube,dims,meas,hierdims)
        else:
            dataframe=df
        
        noOutliers=dataframe[(np.abs(stats.zscore(dataframe.select_dtypes(exclude=["object"]))) < 3).all(axis=1)]

        outliersDF=pd.concat([dataframe,noOutliers]).drop_duplicates(keep=False, inplace=False)
        
        if(displayType=="outliers_only"):
            return outliersDF
        elif(displayType=="without_outliers"):
            return noOutliers

    def ExternalComparison(self,cube=[],dims=[],meas=[],hierdims=[],df=pd.DataFrame(),dims_to_compare=[],meas_to_compare="",comp_type=""):
        """
        ExternalComparison - comparison of numeric values related to textual values within multiple columns
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
        df: dataframe
            if data is already retrieved from SPARQL endpoint, dataframe itself can
            be provided
        dims_to_compare: list[str]
            dimensions, which textual values are bound to be investigated
        meas_to_compare: str
            measure(numeric column), which values related to dim_to_compare 
            will be processed
        comp_type: str
            type of comparison to be performed
        ...
        Output
        -----------
        Independent from comp_type selected, output data will have additional column with numerical
        column processed in specific way.
        Available comparison types (comp_type):
            diffmax->difference with max value related to specific textual values
            diffmean->difference with arithmetic mean related to specific textual values
            diffmin->difference with minimum value related to specific textual values

        """
        
        if(df.empty):
            dataframe=self.retrieveData(cube,dims,meas,hierdims)
        else:
            dataframe=df
        
        if len(dims_to_compare)!=2:
            return -1
        
        if(comp_type=="sum"):
            return dataframe.groupby(dims_to_compare)[meas_to_compare].sum(axis=1)
        elif(comp_type=="diffmax"):
            dataframe["DiffMax("+str(dims_to_compare).strip("[]")+")"]=dataframe.groupby(dims_to_compare)[meas_to_compare].transform(lambda x: x-x.max())
            return dataframe
        elif(comp_type=="diffavg"):
            dataframe["DiffAvg("+str(dims_to_compare).strip("[]")+")"]=dataframe.groupby(dims_to_compare)[meas_to_compare].transform(lambda x: x-round(x.mean(),2))
            return dataframe
        elif(comp_type=="diffmin"):
            dataframe["DiffMin("+str(dims_to_compare).strip("[]")+")"]=dataframe.groupby(dims_to_compare)[meas_to_compare].transform(lambda x: x-x.min())
            return dataframe
            
    def DissectFactors(self,cube=[],dims=[],meas=[],hierdims=[],df=pd.DataFrame(),dim_to_dissect=""):
        """
        DissectFactors - decomposition of data based on values in dim_to_dissect
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
        df: dataframe
            if data is already retrieved from SPARQL endpoint, dataframe itself can
            be provided
        dim_to_dissect: str
            dimension, based on which input data will be decomposed
        ...
        Output
        -----------
        As an output, data will be decomposed in a form of a dictionary, where each 
        subset have values only related to specific value
        """
        
        if(df.empty):
            dataframe=self.retrieveData(cube,dims,meas,hierdims)
        else:
            dataframe=df
        
        uniqueDimValues=dataframe[dim_to_dissect].unique()
        #dictionary based on unique values from dimension
        dimValueDFDict={elem : pd.DataFrame for elem in uniqueDimValues}

        #decompose data into subset grouped under dim_to_dissect
        for key in dimValueDFDict.keys():
            dimValueDFDict[key]=dataframe[:][dataframe[dim_to_dissect] == key]

        return dimValueDFDict