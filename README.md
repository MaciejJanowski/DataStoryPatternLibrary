# DataStoryPatternLibrabry

Data Story Patterns Library is a repository with pattern analysis designated for Linked Open Statistical Data. Story Patterns were retrieved from literture reserach udenr general subject of "data journalism".

### Installation
```python
pip install datastories
```
Requirements will be automatically installed with package

###Import/Usage 
```python
import datastories.analytical as patterns

patterns.DataStoryPattern(sparqlendpointurl, jsonmetadata)
```
Object created allow to query SPARQL endpoint based on JSON meatadat provided
 

# Patterns Description
<!--ts-->
   * [Measurement and Counting](#MCounting)
   * [League Table](#LTable)
   * [Internal Comprison](#InternalComparison)
   * [Profile Outliers](#ProfileOutliers)
   * [Dissect Factors](#DissectFactors)
   * [Highlight Contrast](#HighlightContrast)
   * [Start Big Drill Down](#StartBigDrillDown)
   * [Start Small Zoom Out](#StartSmallZoomOut)
   * [Analysis By Category](#AnalysisByCategory)
<!--te-->
# MCounting

  Measurement and Counting
  Arithemtical operators applied to whole dataset - basic information regarding data
    
### Attributes
 ```python
 MCounting(self,cube="",dims=[],meas=[],hierdims=[],count_type="raw",df=pd.DataFrame() )
 ```
  Parameter                 | Type       | Description   |	
  | :------------------------ |:-------------:| :-------------|
  | cube	       |```	String     ```   | Cube, which dimensions and measures will be investigated
  | dims	       |```	  list[String]     ```   | List of dimensions (from cube) to take into investigation
  | meas	       |	    ```  list[String]  ```      | List of measures (from cube) to take into investigation
  | hierdims	       |```  dict{hierdim:{"selected_level":[value]}}  ```        | Hierarchical Dimesion with selected hierarchy level to take into investigation
  | count_type	       |	```String```         | Type of Count to perform
  | df	       |```	DataFrame      ```    |  DataFrame object, if data is already retrieved from endpoint
 
### Output
Based on count_type value

|Count_type                |  Description   |	
  | ------------------------ | -------------|
  | raw| data without any analysis performed|
  | sum| sum across all numeric columns|
  | mean| mean across all numeric columns|
  | min| minimum values from all numeric columns|
  | max| maximum values from all numeric columns|
  | count| amount of records|


# LTable

  LeagueTable - sorting and extraction specific amount of records
    
### Attributes
 ```python
 LTable(self,cube=[],dims=[],meas=[],hierdims=[], columns_to_order="", order_type="asc", number_of_records=20,df=pd.DataFrame())
 ```
  Parameter                 | Type       | Description   |	
  | :------------------------ |:-------------:| :-------------|
  | cube	       |```	String     ```   | Cube, which dimensions and measures will be investigated
  | dims	       |```	  list[String]     ```   | List of dimensions (from cube) to take into investigation
  | meas	       |	    ```  list[String]  ```      | List of measures (from cube) to take into investigation
  | hierdims	       |```  dict{hierdim:{"selected_level":[value]}}  ```        | Hierarchical Dimesion with selected hierarchy level to take into investigation
  | columns_to_order	       |	```list[String]```         | Set of columns to order by
  | order_type	       |	```String```         | Type of order (asc/desc)
  | number_of_records	       |	```Integer```         | Amount of records to retrieve
  | df	       |```	DataFrame      ```    |  DataFrame object, if data is already retrieved from endpoint
 
### Output
Based on sort_type value

|Sort_type                |  Description   |	
  | ------------------------ | -------------|
  | asc|ascending order based on columns provided in ```columns_to_order```|
  | desc|descending order based on columns provided in ```columns_to_order```|


# InternalComparison

  InternalComparison - comparison of numeric values related to textual values within one column
    
### Attributes
 ```python
 def InternalComparison(self,cube="",dims=[],meas=[],hierdims=[],df=pd.DataFrame(), dim_to_compare="",meas_to_compare="",comp_type="")
 ```
  Parameter                 | Type       | Description   |	
  | :------------------------ |:-------------:| :-------------|
  | cube	       |```	String     ```   | Cube, which dimensions and measures will be investigated
  | dims	       |```	  list[String]     ```   | List of dimensions (from cube) to take into investigation
  | meas	       |	    ```  list[String]  ```      | List of measures (from cube) to take into investigation
  | hierdims	       |```  dict{hierdim:{"selected_level":[value]}}  ```        | Hierarchical Dimesion with selected hierarchy level to take into investigation
  | df	       |```	DataFrame      ```    |  DataFrame object, if data is already retrieved from endpoint
  | dim_to_compare	       |	```String```         | Dimension, which values will be investigated
  | meas_to_compare	       |	```String```         | Measure, which numeric values related to ```dim_to_compare``` will be processed
  | comp_type	       |	```String```         | Type of comparison to perform
 
### Output 
Independent from ```comp_type``` selected, output data will have additional column with numerical column ```meas_to_compare``` processed in specific way.

Available types of comparison ```comp_type```
|Comp_type                |  Description   |	
  | ------------------------ | -------------|
  | diffmax| difference with max value related to specific textual value|
  | diffmean| difference with arithmetic mean related to specific textual values|
  | diffmin|difference with minimum value related to specific textual value|

# ProfileOutliers

  ProfileOutliers - detection of unusual values within data (anomalies)
    
### Attributes
 ```python
 def ProfileOutliers(self,cube=[],dims=[],meas=[],hierdims=[],df=pd.DataFrame(), displayType="outliers_only")
 ```
  Parameter                 | Type       | Description   |	
  | :------------------------ |:-------------:| :-------------|
  | cube	       |```	String     ```   | Cube, which dimensions and measures will be investigated
  | dims	       |```	  list[String]     ```   | List of dimensions (from cube) to take into investigation
  | meas	       |	    ```  list[String]  ```      | List of measures (from cube) to take into investigation
  | hierdims	       |```  dict{hierdim:{"selected_level":[value]}}  ```        | Hierarchical Dimesion with selected hierarchy level to take into investigation
  | df	       |```	DataFrame      ```    |  DataFrame object, if data is already retrieved from endpoint
  | displayType	       |	```String```         | What information display are bound to display (with/without anomalies)

### Output 
Pattern analysis using ```python scipy``` library will perform quick exploration in serach of unusual values within data.

Based on ```displayType``` parameter data will be displayed with/without ddetected unusual values.

Available types of displaying ```displayType```
|DisplayType                |  Description   |	
  | ------------------------ | -------------|
  | outliers_only| returns rows from dataset where unusual values were detected|
  | without_outliers| returns dataset with excluded rows where unusual values were detected|


# DissectFactors

  DissectFactors - decomposition of data based on values in dim_to_dissect
    
### Attributes
 ```python
 def DissectFactors(self,cube="",dims=[],meas=[],hierdims=[],df=pd.DataFrame(),dim_to_dissect="")
 ```
  Parameter                 | Type       | Description   |	
  | :------------------------ |:-------------:| :-------------|
  | cube	       |```	String     ```   | Cube, which dimensions and measures will be investigated
  | dims	       |```	  list[String]     ```   | List of dimensions (from cube) to take into investigation
  | meas	       |	    ```  list[String]  ```      | List of measures (from cube) to take into investigation
  | hierdims	       |```  dict{hierdim:{"selected_level":[value]}}  ```        | Hierarchical Dimesion with selected hierarchy level to take into investigation
  | df	       |```	DataFrame      ```    |  DataFrame object, if data is already retrieved from endpoint
  | dim_to_dissect	       |	```String```         | Based on which dimension data should be decomposed

### Output 
As an output, data will be decomposed in a form of a dictionary, where each subset have values only related to specific value.
Dictionary of subdataset will be constructed as a series of paiers where key per each susbet will values from ```dim_to_dissect```
and this key value will be data, where yhis key value was occurring.


# HighlightContrast

  HighlightContrast - partial difference within values related to one textual column
    
### Attributes
 ```python
 def HighlightContrast(self,cube="",dims=[],meas=[],hierdims=[],df=pd.DataFrame(),dim_to_contrast="",contrast_type="",meas_to_contrast="")
 ```
  Parameter                 | Type       | Description   |	
  | :------------------------ |:-------------:| :-------------|
  | cube	       |```	String     ```   | Cube, which dimensions and measures will be investigated
  | dims	       |```	  list[String]     ```   | List of dimensions (from cube) to take into investigation
  | meas	       |	    ```  list[String]  ```      | List of measures (from cube) to take into investigation
  | hierdims	       |```  dict{hierdim:{"selected_level":[value]}}  ```        | Hierarchical Dimesion with selected hierarchy level to take into investigation
  | df	       |```	DataFrame      ```    |  DataFrame object, if data is already retrieved from endpoint
  | dim_to_contrast	       |	```String```         | Textual column, from which values will be contrasted
  | meas_to_contrast	       |	```String```         | Numerical column, which values are contrasted
  | contrast_type	       |	```String```         | Type of contrast to present
 
### Output 
Independent from ```contrast_type``` selected, output data will have additional column with numerical column ```meas_to_contrast``` processed in specific way.

Available types of comparison ```contrast_type```
|Contrast_type                |  Description   |	
  | ------------------------ | -------------|
  | partofwhole| difference with max value related to specific textual value|
  | partofmax| difference with arithmetic mean related to specific textual values|
  | partofmin|difference with minimum value related to specific textual value|




# StartBigDrillDown

  StartBigDrillDown - data retrieval from multiple hierachical levels.

  This pattern can be only applied to data not stored already in DataFrame
    
### Attributes
 ```python
 def StartBigDrillDown(self,cube="",dims=[],meas=[],hierdim_drill_down=[])
 ```
  Parameter                 | Type       | Description   |	
  | :------------------------ |:-------------:| :-------------|
  | cube	       |```	String     ```   | Cube, which dimensions and measures will be investigated
  | dims	       |```	  list[String]     ```   | List of dimensions (from cube) to take into investigation
  | meas	       |	    ```  list[String]  ```      | List of measures (from cube) to take into investigation
  | hierdim_drill_down	       |```  dict{hierdim:list[str]} ```        | Hierarchical dimension with list of hierarchy levels to inspect
  

### Output 
As an output, data will be retrieved in a form of a dictionary, where each dataset will be retrieved from different hierachy level. List will be provided in```hierdim_drill_down```. Hierachy levels provided by in parameter will automatically sorted in order from most general to most detailed level based on metadata provided.


# StartSmallZoomOut

  StartSmallZoomOut - data retrieval from multiple hierachical levels.

  This pattern can be only applied to data not stored already in DataFrame
    
### Attributes
 ```python
 def StartSmallZoomOut(self,cube="",dims=[],meas=[],hierdim_zoom_out=[])
 ```
  Parameter                 | Type       | Description   |	
  | :------------------------ |:-------------:| :-------------|
  | cube	       |```	String     ```   | Cube, which dimensions and measures will be investigated
  | dims	       |```	  list[String]     ```   | List of dimensions (from cube) to take into investigation
  | meas	       |	    ```  list[String]  ```      | List of measures (from cube) to take into investigation
  | hierdim_zoom_out	       |```  dict{hierdim:list[str]} ```        | Hierarchical dimension with list of hierarchy levels to inspect
  

### Output 
As an output, data will be retrieved in a form of a dictionary, where each dataset will be retrieved from different hierachy level. List will be provided in```hierdim_zoom_out```. Hierachy levels provided by in parameter will automatically sorted in order from most detaile to most general level based on metadata provided.


# AnalysisByCategory

  AnalysisByCategory - ecomposition of data based on values in dim_for_category with analysis performed on each susbet
    
### Attributes
 ```python
 def AnalysisByCategory(self,cube="",dims=[],meas=[],hierdims=[],df=pd.DataFrame(),dim_for_category="",meas_to_analyse="",analysis_type="min"):
 ```
  Parameter                 | Type       | Description   |	
  | :------------------------ |:-------------:| :-------------|
  | cube	       |```	String     ```   | Cube, which dimensions and measures will be investigated
  | dims	       |```	  list[String]     ```   | List of dimensions (from cube) to take into investigation
  | meas	       |	    ```  list[String]  ```      | List of measures (from cube) to take into investigation
  | hierdims	       |```  dict{hierdim:{"selected_level":[value]}}  ```        | Hierarchical Dimesion with selected hierarchy level to take into investigation
  | df	       |```	DataFrame      ```    |  DataFrame object, if data is already retrieved from endpoint
  | dim_for_category	       |	```String```         |  Dimension, based on which input data will be categorised
  | meas_to_analyse	       |	```String```         | Measure, which will be analysed
  | analysis_type	       |	```String```         | Type of analysis to perform
 
### Output 
As an output, data will be decomposed in a form of a dictionary, where each subset have values only related to specific value. Such subset will get analysed based on ```analysis_type``` parameter

Available types of analysis ```analysis_type```
|Analysis_type                |  Description   |	
  | ------------------------ | -------------|
  | min| Minimum per each category|
  | max| Maximum per each category|
  | mean|Arithmetical mean per each category|
  | sum|Total value from each category|


# ExploreIntersection TODO

#NarratingChangeOT TODO
