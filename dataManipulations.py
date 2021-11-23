import io
import csv
import pandas as pd
import numpy as np
from dataclasses import make_dataclass

class product(object):
	"""docstring for product"""
	def __init__(self):
		super(product, self).__init__()

#reding csv file using Pandas dataframe
	def _read_prod_file_csv(self, filePath):
		self.path = filePath
		df = pd.read_csv(self.path)
		print(df)

	def _create_pandas_data_frame(self):
		df = pd.DataFrame([['Jack', 24], ['Rose', 22]], columns = ['Name', 'Age'])
		print(df)


	def _write_to_csv_file(self):
		df = pd.DataFrame([[1,'Jack', 24], [2,'Rose', 22],[3,'Mahesh', 23] , [4, 'Rachel', 25]], columns = ['id','Name', 'Age'])
		df.to_csv('D:\GAYANI\employees.csv')

	def _creating_data_frames(self):
		#Constructing DataFrame from a dictionary.
		d = {'col1': [1, 2], 'col2': [3, 4]}
		df = pd.DataFrame(data=d)
		#printing the datatypes for the columns
		print(df.dtypes) 
		print(df)

		#To enforce a single dtype:
		df = pd.DataFrame(data=d, dtype=np.int8)
		print(df.dtypes)

		#Constructing DataFrame from numpy ndarray:
		df2 = pd.DataFrame(np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]), columns=['a', 'b', 'c'])
		print(df2)

		data = np.array([(1, 2, 3), (4, 5, 6), (7, 8, 9)], dtype=[("a", "i4"), ("b", "i4"), ("c", "i4")])
		print(data)
		df3 = pd.DataFrame(data, columns=['c', 'a'])
		print(df3)

		#Constructing DataFrame from dataclass:
		Point = make_dataclass("Point", [("x", int), ("y", int)])
		df4 = pd.DataFrame([Point(0, 0), Point(0, 3), Point(2, 3)])
		print(df4)


	def _accessing_data_frames(self):
		df = pd.DataFrame([[0, 2, 3], [0, 4, 1], [10, 20, 30]],
                  index=[4, 5, 6], columns=['A', 'B', 'C']) #index refer to the line number and in this df line number starts with 4

		print(df) 
		#print the specific values / get specific row/column pair
		print(df.at[4, 'B'])

		#set row column pair
		df.at[4, 'B'] = 10

		#setting the column values
		print(df.at[4, 'B'])
		print(df.loc[5].at['B'])

		#creating dataframe with different data types
		df = pd.DataFrame({'float': [1.0],
                   'int': [1],
                   'datetime': [pd.Timestamp('20180310')],
                   'string': ['foo']})

		print(df)
		print(df.dtypes)

		#creating more dataframes
		int_values = [1, 2, 3, 4, 5]
		text_values = ['alpha', 'beta', 'gamma', 'delta', 'epsilon']
		float_values = [0.0, 0.25, 0.5, 0.75, 1.0]
		df = pd.DataFrame({"int_col": int_values, "text_col": text_values,
                  "float_col": float_values})
		print(df)

		print(df.info(verbose=True)) #will display all the informtion related to dataframe

		#Prints a summary of columns count and its dtypes but not per column information:
		print(df.info(verbose=False))

		#Pipe output of DataFrame.info to buffer instead of sys.stdout, get buffer content and writes to a text file

        # buffer = io.StringIO()
        # df.info(buf=buffer)
        # s = buffer.getvalue()
        # with open("df_info.txt", "w",
        #           encoding="utf-8") as f:  
        #     f.write(s)

		random_strings_array = np.random.choice(['a', 'b', 'c'], 10 ** 6)
		df = pd.DataFrame({
			'column_1': np.random.choice(['a', 'b', 'c'], 10 ** 6),
			'column_2': np.random.choice(['a', 'b', 'c'], 10 ** 6),
			'column_3': np.random.choice(['a', 'b', 'c'], 10 ** 6)
		})
		print(df.info())
		print(df.info(memory_usage='deep'))

		#pandas.DataFrame.select_dtypes
		df = pd.DataFrame({'a': [1, 2] * 3,
                   'b': [True, False] * 3,
                   'c': [1.0, 2.0] * 3})

		print("pandas.DataFrame.select_dtypes")
		print(df)
		print(df.select_dtypes(include='bool'))
		print(df.select_dtypes(include='float64'))
		print(df.select_dtypes(exclude='int64'))
		print(df.select_dtypes(exclude=['int64' , 'float64']))
		print(df.select_dtypes(include=['int64' , 'float64']))


		#An example of an actual empty DataFrame. Notice the index is empty:
		print('An example of an actual empty DataFrame. Notice the index is empty:')
		#defining an empty dataframe
		df_empty = pd.DataFrame({'A' : []})
		print(df_empty)

		print(df_empty.empty)

		print('If we only have NaNs in our DataFrame, it is not considered empty! We will need to drop the NaNs to make the DataFrame empty:')
		df = pd.DataFrame({'A' : [np.nan]})
		print(df)

		print(df.empty) #will receive as False as nan is not consideded as empty
		print(df.dropna().empty) #will receive empty as we drop nan before checking empty

		print('pandas.DataFrame.flags')
		df = pd.DataFrame({"A": [1, 2]})
		print(df)
		#print(df.flags)

		print('pandas.DataFrame.iat')
		df = pd.DataFrame([[0, 2, 3], [0, 4, 1], [10, 20, 30]],
                  columns=['A', 'B', 'C'])

		print(df)
		#Get value at specified row/column pair
		print(df.iat[1, 2])

		#set the row/column pair
		df.iat[1, 2] = 10
		print(df)

		print(df.iat[1, 2])
		#Get value within a series
		print(df.loc[0].iat[1])


		print('pandas.DataFrame.iloc')
		mydict = [{'a': 1, 'b': 2, 'c': 3, 'd': 4},
          {'a': 100, 'b': 200, 'c': 300, 'd': 400},
          {'a': 1000, 'b': 2000, 'c': 3000, 'd': 4000 }]
		df = pd.DataFrame(mydict)
		print(df)

		print(type(df.iloc[0]))

p = product()
#p._read_prod_file_csv('D:\GAYANI\prodc.csv')
#p._create_pandas_data_frame()
#p._write_to_csv_file()
#p._creating_data_frames()
p._accessing_data_frames()