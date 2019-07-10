from SPARQLWrapper import SPARQLWrapper, SPARQLWrapper2, JSON, JSONLD, CSV, TSV, N3, RDF, RDFXML, TURTLE
import sparql_dataframe
import pandas as pd
import numpy as np
from scipy import stats


class DataStoryPattern():

    def __init__(self,sparqlEndpoint,jsonmetadata):
        self.sparqlEndpoint=sparqlEndpoint
        self.metaDataDict=jsonmetadata

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

    def MCounting(self,cube="",dims=[],meas=[],hierdims=[],count_type="raw",df=pd.DataFrame() ): 
        """
        MCounting -> MeasurementCounting - arithemtical operators applied to whole dataset
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
        try:
            if(df.empty):
                dataframe=self.retrieveData(cube,dims,meas,hierdims)
            else:
                dataframe=df
        except Exception as e:
            raise ValueError("Wrong dimension/measure given: "+e)
        try:
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
            else:
                raise ValueError("Wrong type of count! :"+count_type)
        except Exception as e:
            raise ValueError("Data not eglible for analysis:" +e)

    def LTable(self,cube="",dims=[],meas=[],hierdims=[], columns_to_order=[], order_type="asc", number_of_records=20,df=pd.DataFrame()):
        """
        LTable -> LeagueTable - sorting and extraction specific amount of records
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
        try:
            if(df.empty):
                dataframe=self.retrieveData(cube,dims,meas,hierdims)
            else:
                dataframe=df
        except Exception as e:
            raise ValueError("Wrong dimension/measure given: "+e)
        try:
            if(order_type=="asc"):
                return dataframe.sort_values(by=columns_to_order,ascending=True).head(number_of_records)
            elif(order_type=="desc"):
                return dataframe.sort_values(by=columns_to_order, ascending=False).head(number_of_records)
            else:
                raise ValueError("Wrong order type: "+order_type)
        except Exception as e:
            raise ValueError("Data not eglible for analysis:"+e)



    def InternalComparison(self,cube="",dims=[],meas=[],hierdims=[],df=pd.DataFrame(), dim_to_compare="",meas_to_compare="",comp_type=""):
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

        try:
            if(df.empty):
                dataframe=self.retrieveData(cube,dims,meas,hierdims)
            else:
                dataframe=df
        except Exception as e:
            raise ValueError("Wrong dimension/measure given: "+e)
        try:
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
            else:
                raise ValueError("Wrong type of comparison selected!: "+comp_type)
        except Exception as e:
            raise ValueError("Data not eglible for analysis: "+e)


    def ProfileOutliers(self,cube="",dims=[],meas=[],hierdims=[],df=pd.DataFrame(), displayType="outliers_only"):
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
        try:
            if(df.empty):
                dataframe=self.retrieveData(cube,dims,meas,hierdims)
            else:
                dataframe=df
        except Exception as e:
            raise ValueError("Wrong dimension/measure given: "+e)
        
        try:
            noOutliers=dataframe[(np.abs(stats.zscore(dataframe.select_dtypes(exclude=["object"]))) < 3).all(axis=1)]

            outliersDF=pd.concat([dataframe,noOutliers]).drop_duplicates(keep=False, inplace=False)
            
            if(displayType=="outliers_only"):
                return outliersDF
            elif(displayType=="without_outliers"):
                return noOutliers
            else:
                raise ValueError("Wrong display Type:"+ displayType)
        except Exception as e:
            raise ValueError("Data not eglible for analysis: " + e)


    def DissectFactors(self,cube="",dims=[],meas=[],hierdims=[],df=pd.DataFrame(),dim_to_dissect=""):
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
        
        try:
            if(df.empty):
                dataframe=self.retrieveData(cube,dims,meas,hierdims)
            else:
                dataframe=df
        except Exception as e:
            raise ValueError("Wrong dimension/measure given: "+e)
        
        try:
            uniqueDimValues=dataframe[dim_to_dissect].unique()
            #dictionary based on unique values from dimension
            dimValueDFDict={elem : pd.DataFrame for elem in uniqueDimValues}

            #decompose data into subset grouped under dim_to_dissect
            for key in dimValueDFDict.keys():
                dimValueDFDict[key]=dataframe[:][dataframe[dim_to_dissect] == key]

            return dimValueDFDict
        except Exception as e:
            raise ValueError("Data not eglible for analysis:"+e)


    def HighlightContrast(self,cube="",dims=[],meas=[],hierdims=[],df=pd.DataFrame(),dim_to_contrast="",contrast_type="",meas_to_contrast=""):
        """
        HighlightContrast - partial difference within values related to one textual column
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
        dim_to_contrast: str
            textual column, from which values will be contrasted
        meas_to_contrast: str
            numerical column, which values are contrasted
        contrast_type: str
            type of contrast to present

        ...
        Output
        -----------
        Output data will have additional column with contrast presented.

        Available contrast_type
            partofwhole->what part in total value is each value. 
            partofmax->how big part of max value each value is
            partofmin->how big part of min value each value is
        """
        
        
        try:
            if(df.empty):
                dataframe=self.retrieveData(cube,dims,meas,hierdims)
            else:
                dataframe=df
        except Exception as e:
            raise ValueError("Wrong dimension/measure given: "+e)
        
        try:
            if(contrast_type=="partofwhole"):
                dataframe["PartOfWhole"]=dataframe.groupby(dim_to_contrast)[meas_to_contrast].transform(lambda x: x/x.sum()).round(2)
                return dataframe
            elif(contrast_type=="partofmax"):
                dataframe["PartOfMax"]=dataframe.groupby(dim_to_contrast)[meas_to_contrast].transform(lambda x: x/x.max()).round(2)
                return dataframe
            elif(contrast_type=="partofmin"):
                dataframe["PartOfMin"]=dataframe.groupby(dim_to_contrast)[meas_to_contrast].transform(lambda x: x/x.min()).round(2)
                return dataframe
            else:
                raise ValueError("Wrong contrast_type specified")
        except Exception as e:
            raise ValueError("Data not eglible for analysis"+e)

    def StartBigDrillDown(self,cube="",dims=[],meas=[],hierdim_drill_down=[]):
        """
        StartBigDrillDown - data retrieval from multiple hierachical levels
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
        hierdim_drill_down: dict{hierdim:list[str]}
            hierarchical dimension with list of hierarchy levels to inspect
        ...
        Output
        -----------
        As an output, data will be retrieved as a series of dataset retrieved from different hierarchy levels
        form most general one to most detailed one
        """

        if not (cube and dims and meas and hierdim_drill_down):
            raise ValueError("Wrong dimension/measure/cube specified")
        try:
            for el in hierdim_drill_down:
                hierdimLevels=sorted(hierdim_drill_down[el], key=self.metaDataDict[cube]["hierarchical_dimensions"][el]["dimension_levels"].get)
                hierdimDict={ hier_level: pd.DataFrame for hier_level in hierdimLevels}
                for dimlevel in hierdimDict.keys():
                    hierdims={el:{"selected_level":dimlevel}}
                    hierdimDict[dimlevel]=self.retrieveData(cube,dims,meas,hierdims)

            return hierdimDict
        
        except Exception as e:
            raise ValueError("Data retrieval from multiple levels failed:" +e)
    

    def StartSmallZoomOut(self,cube="",dims=[],meas=[],hierdim_zoom_out=[]):
        """
         StartSmallZoomOut - data retrieval from multiple hierachical levels
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
        hierdim_zoom_out: dict{hierdim:list[str]}
            hierarchical dimension with list of hierarchy levels to inspect
        ...
        Output
        -----------
        As an output, data will be retrieved as a series of dataset retrieved from different hierarchy levels from most detailed
         one to most general one
        """

        if not (cube and dims and meas and hierdim_zoom_out):
            raise ValueError("Wrong dimension/measure/cube specified")
        try:
            for el in hierdim_zoom_out:
                hierdimLevels=sorted(hierdim_zoom_out[el], key=self.metaDataDict[cube]["hierarchical_dimensions"][el]["dimension_levels"].get)
                hierdimDict={ hier_level: pd.DataFrame for hier_level in hierdimLevels}
                for dimlevel in hierdimDict.keys():
                    hierdims={el:{"selected_level":dimlevel}}
                    hierdimDict[dimlevel]=self.retrieveData(cube,dims,meas,hierdims)

            return hierdimDict
        
        except Exception as e:
            raise ValueError("Data retrieval from multiple levels failed:" +e)
    

    def AnalysisByCategory(self,cube="",dims=[],meas=[],hierdims=[],df=pd.DataFrame(),dim_for_category="",meas_to_analyse="",analysis_type="min"):
        """
        AnalysisByCategory - decomposition of data based on values in dim_for_category with analysis performed on each susbet
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
        dim_for_category: str
            dimension, based on which input data will be categorised
        meas_to_analyse: str
            numerical property to analyse
        analysis_type:str
            type of analysis to perform
        ...
        Output
        -----------
        As an output, data will be decomposed in a form of a dictionary, where each 
        subset have values only related to specific value. Such subset will get analysed

        Available values for analysis_type
            min-> minimum for each category
            max -> maximum for each category
            mean -> arithmetical mean per each category
            sum -> total value per each category
        """
        try:
            if(df.empty):
                dataframe=self.retrieveData(cube,dims,meas,hierdims)
            else:
                dataframe=df
        except Exception as e:
            raise ValueError("Wrong dimension/measure given: "+e)


        try:
            if not (dim_for_category): ##If not specified
                dim_for_category=dims[0]

            uniqueDimValues=dataframe[dim_for_category].unique()

            analysisDict={elem:{analysis_type:0} for elem in uniqueDimValues}

            if(analysis_type=="min"):
                for dimvalue in uniqueDimValues:
                    tempdataframe=dataframe[:][dataframe[dim_for_category] == dimvalue]
                    analysisDict[dimvalue][analysis_type]=tempdataframe[meas_to_analyse].min()
                    
                return analysisDict
            
            elif(analysis_type=="max"):
                for dimvalue in uniqueDimValues:
                    tempdataframe=dataframe[:][dataframe[dim_for_category] == dimvalue]
                    analysisDict[dimvalue][analysis_type]=tempdataframe[meas_to_analyse].max()
                    
                return analysisDict
            
            elif(analysis_type=="mean"):
                for dimvalue in uniqueDimValues:
                    tempdataframe=dataframe[:][dataframe[dim_for_category] == dimvalue]
                    analysisDict[dimvalue][analysis_type]=tempdataframe[meas_to_analyse].mean()
                    
                return analysisDict

            elif(analysis_type=="sum"):
                for dimvalue in uniqueDimValues:
                    tempdataframe=dataframe[:][dataframe[dim_for_category] == dimvalue]
                    analysisDict[dimvalue][analysis_type]=tempdataframe[meas_to_analyse].sum()
                    
                return analysisDict
            else:
                raise ValueError("Wrong type of analysis: "+analysis_type)
        
        except Exception as e:
            raise ValueError("Data not eglible for analysis "+e)

    
    def ExploreIntersection(self, dim_to_explore=""):
        """
        Explore intersection - investigating existing of given dimension across provided meta

        ...
        Attributes
        ----------
            dim_to_explore -> dimension, which existence within enpoint is going to be investigated
        ...
        Output
        ---------
        Pattern will return series of datasets, where each will represent occurence of dim_to_explore in one cube
        """

        isHierarchical=False #Flag for hierarchy type
        cube_occurences=[] #List of cube where dimension exists

###Inspection of dimension occurences
        try:
            for cube in self.metaDataDict.keys():
                if dim_to_explore in self.metaDataDict[cube]["hierarchical_dimensions"]:
                    isHierarchical=True
                    break #If dimension is found once as hierarchical it is hierarchical across all cubes

            if not isHierarchical:
                for cube in self.metaDataDict.keys():
                    if dim_to_explore in self.metaDataDict[cube]["dimensions"]:
                        cube_occurences.append(cube) ##
            else:
                for cube in self.metaDataDict.keys():
                    if dim_to_explore in self.metaDataDict[cube]["hierarchical_dimensions"]:
                        cube_occurences.append(cube)

            cubesToRetrieveData=cube_occurences
            ###Dicitonary per each cube 
            cubesIntersectDataDict={cube : pd.DataFrame for cube in cubesToRetrieveData}

        except Exception as e:
            raise ValueError("Wrong dimension specified:" +e)
     
       
###Retrieve data from each cube, where dimension cna be found
        try:
            for cube in cubesToRetrieveData:
                dimensions=self.metaDataDict[cube]["dimensions"].keys()
                measures=self.metaDataDict[cube]["measures"].keys()
                cubesIntersectDataDict[cube]=self.retrieveData(cube,dimensions,measures)
            return cubesIntersectDataDict
        except Exception as e:
            raise ValueError("Data retrieval failed:" +e)

    def NarrChangeOT(self,cube="",dims=[],meas=[],hierdims=[],df=pd.DataFrame(),meas_to_narrate="",narr_type=""):
        try:
            if(df.empty):
                dataframe=self.retrieveData(cube,dims,meas,hierdims)
            else:
                dataframe=df
        except Exception as e:
            raise ValueError("Wrong dimension/measure given: "+e)
        
        if len(meas_to_narrate) != 2: #there has to be two measures 
            raise ValueError("There has to be 2 measures to narrate")
        try:

            if (narr_type=="percchange"): #percentage change
                dataframe["PercChange"]=((dataframe[meas_to_narrate[1]]-dataframe[meas_to_narrate[0]])/dataframe[meas_to_narrate[0]] *100).round(2)
                return dataframe
            elif(narr_type=="diffchange"): ##Numeric difference
                dataframe["DiffChange"]=((dataframe[meas_to_narrate[1]]-dataframe[meas_to_narrate[0]])).round(2)
                return dataframe
        except Exception as e:
            raise ValueError("Narration failed:"+e)



    


