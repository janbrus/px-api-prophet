"""
coding: utf-8

@authour: Christine K. Kaushal

28.09.18

Inspired by:

https://github.com/hmelberg/stats-to-pandas/blob/master/stats_to_pandas/__init__.py

https://github.com/eurostat/prophet

"""

from __future__ import print_function

import pandas as pd

import requests

import ast

from pyjstat import pyjstat

from collections import OrderedDict

from ipywidgets import widgets

from IPython.display import display

# todo: consider using jsonstat instead of pyjstat

class API_to_data:

    def __init__(self, language='en', base_url='http://data.ssb.no/api/v0'):

        """

        Parameters:

        -----------

        language: string
            default in Statistics Norway: 'en' (Search for English words)
            optional in Statistics Norway: 'no' (Search for Norwegian words)

        url: string
            default in Statistics Norway: 'http://data.ssb.no/api/v0'
            different defaults can be specified

        """

        self.language = language
        self.burl = base_url
        self.furl = None
        self.variables = None
        self.time = None

    def search(self, phrase):
        """
        Search for tables that contain the phrase in Statistics Norway.
        Returns a pandas dataframe with the results.

        Not case sensitive.
        Language sensitive (specified in the language option)

        Example
        -------

        df = search("income")

        Parameters
        ----------

        phrase: string
            The phrase can contain several words (space separated): 
            search("export Norwegian parrot")

            It also supports trucation: 
            search("pharma*")

            """

        # todo: make converter part of the default specification only for statistics norway
        convert = {'æ' : '%C3%A6', 'Æ' : '%C3%86', 'ø' : '%C3%B8', 'Ø' : '%C3%98', 'å' : '%C3%A5', 'Å' : '%C3%85',
                   '"' : '%22', '(' : '%28', ')' : '%29', ' ' : '%20'}

        search_str = '{base_url}/{language}/table/?query={phrase}'.format(base_url = self.burl, language = self.language, phrase = phrase)

        for k, v in convert.items():
            search_str = search_str.replace(k, v)

        df = pd.read_json(search_str)

        if len(df) == 0:
            print("No match")
            return df

        # make the dataframe more readable
        # (is it worth it? increases vulnerability. formats may differ and change)
        # todo: make search and format conditional on the database being searched  

        # split the table name into table id and table text

        df['table_id'] = df['title'].str.split(':').str.get(0)
        df['table_title'] = df['title'].str.split(':').str.get(1)
        del df['title']

        # make table_id the index, visually more intuitive with id as first column
        df = df.set_index('table_id')

        # change order of columns to make it more intuitive (table_title is first) 
        cols = df.columns.tolist()
        cols.sort(reverse = True)
        df = df[cols[:-2]]

        return df

    def get_variables(self, table_id = None):
        """
        Returns a list. 

        Each element of the list is a dictionary that provides more 
        information about a variable. 

        For instance, one variable may contain information about the
        different years that are available.

        Parameters
        ----------

        table_id: string
            the unique table_id number, a string including leading zeros.

        """
        try:
            numb = int(table_id)
            if len(str(numb))==4:
                numb = '0'+str(numb)

        except ValueError:
            print('table_id mus be of type integer')

        if self.furl is None:
            self.furl = '{base_url}/{language}/table/{table_id}'.format(base_url = self.burl, language = self.language, 
                                                                             table_id = numb)

        df = pd.read_json(self.furl)
        variables = [dict(values) for values in df.iloc[:, 1]]

        return variables

    def select(self, table_id):
        """
        Selects a table based on the table_id and returns a widget container 
        in which the user can select the set of variables and values to be 
        included in the final table.

        Example
        --------
        box = select(table_id = '10714')

        Parameters
        ----------  

        table_id : string 
            the id of the desired table

        """

        # get a list with dictionaries containing information about each variable
        self.variables = self.get_variables(table_id = table_id)

        table_info = pd.read_json(self.furl)
        table_title = table_info.iloc[0, 0]

        # get number of variables (ok, childish approach, can be simplified!)
        nvars = len(self.variables)
        var_list = list(range(nvars))

        # a list of dictionaries of the values available for each variable
        option_list = [OrderedDict(zip(self.variables[var]['valueTexts'],
                                       self.variables[var]['values']))
                       for var in var_list]

        # create a selection widget for each variable
        # todo: skip widget or make it invisible if there is only one option?
        # todo: make first alternative a default selection initially for all tables?
        # todo: add buttons for selecting "all", "latest" , "first" and "none"

        selection_widgets = [widgets.widget_selection.SelectMultiple(
                                options = option_list[var], 
                                rows = 8,
                                layout ={'width' : '500px'}
                                )
                             for var in var_list]

        # put all the widgets in a container
        variables_container = widgets.Tab(selection_widgets)

        # label each container with the variable label 
        for var in var_list:
            title = str(self.variables[var]['text'])
            variables_container.set_title(var, title)

        # build widgets and put in one widget container
        headline = widgets.Label(value = table_title, color = 'blue')

        endline = widgets.Label(value = '''Select category and click on elements 
            to be included in the table (CTRL-A selects "all")''')

        url_text = widgets.Label(value = self.furl)

        from IPython.display import display
        button = widgets.Button(description="Click when finished")

        selection_container = widgets.VBox([headline, 
                                            endline, 
                                            variables_container, 
                                            url_text,
                                            button])

        selection_container.layout.border = '3px grey solid'

        def clicked(b):
            print('Info is saved. You can now run the rest of the code :)')

        button.on_click(clicked)
        return selection_container

    def get_json(self, box=None, out = 'dict'):
        """
        Takes a widget container as input (where the user has selected varables) 
        and returns a json dictionary or string that will fetch these variables. 

        The json follows the json-stat format.

        Parameters
        ----------

        box : widget container 
            name of widget box with the selected variables

        out : string 
            default: 'dict', options: 'str'

            The json can be returned as a dictionary or a string.
            The final end query should use a dict, but some may find it useful to
            get the string and revise it before transforming it back to a dict.

        Example
        -------

        json_query = get_json(box)

        """

        table_url = box.children[3].value
        nvars = len(box.children[2].children)
        var_list = list(range(nvars))
        query_element = {}

        # create a dict of strings, one for each variable that specifies 
        # the json-stat that selects the variables/values

        for x in var_list:
            value_list = str(list(box.children[2].children[x].value))
            query_element[x] = '{{"code": "{code}", "selection": {{"filter": "item", "values": {values} }}}}'.format(
                code = self.variables[x]['code'], 
                values = value_list)
            query_element[x] = query_element[x].replace("\'", '"')

        all_elements = str(list(query_element.values()))
        all_elements = all_elements.replace("\'", "")

        query = '{{"query": {all_elements} , "response": {{"format": "json-stat" }}}}'.format(all_elements = all_elements)

        if out == 'dict':
            query = ast.literal_eval(query)

        # todo: build it as a dictionary to start with (and not a string that is made into a dict as now)
        # todo: add error message if required variables are not selected
        # todo: avoid repeat downloading of same information 
        # eg. get_variables is sometimes used three times before a table is downloaded

        return query

    def to_dict(json_str):
        """
        Transforms a string to a dictionary.

        Note: Will fail if string is not correctly specified.
        """

        # OK, really unnecessary func, but a concession to less experienced users
        # todo: use json module instead, json.dumps()
        query = ast.literal_eval(json_str)
        return query

    def read_box(self, from_box):
        """
        Takes a widget container as input (where the user has selected varables) 
        and returns a pandas dataframe with the values for the selected variables.

        Example
        -------

        df = read_box(box)

        Parameters:
        -----------

        from_box: widget container

        """
        try:

            query = self.get_json(from_box)
            url = from_box.children[3].value
            data = requests.post(url, json = query)
            results = pyjstat.from_json_stat(data.json(object_pairs_hook=OrderedDict))
            label = data.json(object_pairs_hook=OrderedDict)
            return [results[0], label['dataset']['label']]
        except TypeError:
            print('You must make choices in the box!')
        except:
            print('You must make choices in the box!')

    def fiksDato(self, dato):
        hjdat = int(dato[5:6])*3
        hjdat2 = str(hjdat)
        if hjdat < 12:
            dato = dato[0:4] + '-0' + hjdat2
        else:
            dato = dato[0:4] + '-' + hjdat2

        dates = pd.date_range(dato, periods=1, freq='M')
        dato = str(dates[0])

        return dato

    def prepare_dataframe(self, df, val_col='value'):
        """

        Parameters:
        -----------

        time_col : type sting
            Name of time column (for SSB-data, usually 'uke', 'måned', 'år' og 'kvartal' in norwegian)
        val_col : type string
            Name of column containing the values (usually 'value')
        df : typ pandas.DataFrame
            The dataset one want to make forecast of

        """

        self.time = time_col = df.columns[-2]

        if self.language == 'no':
            df_ret = df[[self.time, val_col]]

            if 'M' in df_ret.loc[0, self.time]:
                self.time = 'måned'
                df_ret = df[[self.time, val_col]]
                df_ret.loc[:, self.time] = pd.to_datetime(df[self.time].str.replace('M', '-'))
                freq = 'M'; periods = 12;
            elif 'U' in df_ret.loc[0, self.time]:
                self.time = 'uke'
                df_ret = df[[self.time, val_col]] 
                df_ret.loc[:, self.time] = pd.to_datetime((df[self.time].str.replace('U', '-')).add('-1'), format='%Y-%W-%w')
                freg = 'W'; periods = 52;
            elif 'K' in df_ret.loc[0, self.time]:
                time_col = 'kvartal'
                df_ret = df[[self.time, val_col]] 
                df_ret.loc[:, self.time] = pd.to_datetime(df[self.time].str.replace('K', '-'))
                df_ret.loc[:, self.time] = df[self.time].apply(self.fiksDato)
                freq = 'q'; periods = 4;
            else:
                self.time = 'år'
                df_ret = df[[self.time, val_col]] 
                df_ret.loc[:, self.time] = pd.to_datetime(df[self.time])
                freq = 'y'
        elif self.language == 'en':
            df_ret = df[[self.time, val_col]]

            if 'M' in df_ret.loc[0, self.time]:
                self.time = 'month'
                df_ret = df[[self.time, val_col]]
                df_ret.loc[:, self.time] = pd.to_datetime(df[self.time].str.replace('M', '-'))
                freq = 'M'; periods = 12;
            elif 'U' in df_ret.loc[0, self.time]:
                self.time = 'week'
                df_ret = df[[self.time, val_col]] 
                df_ret.loc[:, self.time] = pd.to_datetime((df[self.time].str.replace('U', '-')).add('-1'), format='%Y-%W-%w')
                freg = 'W'; periods = 52;
            elif 'K' in df_ret.loc[0, self.time]:
                self.time = 'quarter'
                df_ret = df[[self.time, val_col]]
                df_ret.loc[:, self.time] = pd.to_datetime(df[self.time].str.replace('K', '-'))
                df_ret.loc[:, self.time] = df[self.time].apply(self.fiksDato)
                freq = 'q'; periods = 4;
            else:
                self.time = 'year'
                df_ret = df[[self.time, val_col]] 
                df_ret.loc[:, self.time] = pd.to_datetime(df[self.time])
                freq = 'y'

        #the input to `Prophet` is always a `pandas.DataFrame` object, and it must contain two columns: `ds` and `y`:
        df_ret.columns = ['ds', 'y']

        return [df_ret, freq, periods]