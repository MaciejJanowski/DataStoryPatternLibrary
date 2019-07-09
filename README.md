# DataStoryPatternLibrabry

Data Story Patterns Library is a repository with pattern analysis designated for Linked Open Statistical Data. Story Patterns were retrieved from literture reserach udenr general subject of "data journalism".

### Installation
```sh
pip install datastories
```
Requirements will be automatically installe
 

# Patterns Description
<!--ts-->
   * [Measurement and Counting](#MCounting)
   * [League Table](#LTable)
   * [Installation](#installation)
   * [Usage](#usage)
      * [STDIN](#stdin)
      * [Local files](#local-files)
      * [Remote files](#remote-files)
      * [Multiple files](#multiple-files)
      * [Combo](#combo)
      * [Auto insert and update TOC](#auto-insert-and-update-toc)
      * [GitHub token](#github-token)
   * [Tests](#tests)
   * [Dependency](#dependency)
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

| First Header  | Second Header |
| ------------- | ------------- |
| Content Cell  | Content Cell  |
| Content Cell  | Content Cell  |
Count_type                |  Description   |	
  | :------------------------ | :-------------|
  | raw| data without any analysis performed
  | sum| sum across all numeric columns
  | mean| mean across all numeric columns
  | min| minimum values from all numeric columns
  | max| maximum values from all numeric columns
  | count| amount of records


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
 Count_type                 | Description   	
  | :------------------------ | :-------------|
  | asc|ascending order based on columns provided in ```columns_to_order```|
  | desc|descending order based on columns provided in ```columns_to_order```||
 