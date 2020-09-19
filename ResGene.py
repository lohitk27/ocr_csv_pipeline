import csv
import pandas as pd
import os
from cassandra import Cassandra
class Results_generator():
    def __init__(self,seed=2000):
        self.seed = seed
        #self.df_copy
    def csv_generator(self,path1,arg_docname,arg_pagenumber,arg_pagelocation,ocr_output,arg_line_number,arg_line_x,arg_line_y,arg_line_height,arg_line_width,arg_font_size,str_Image_name,,arg_ROI_x,arg_ROI_y,arg_pipeline_num,arg_orientation,bool_underline=False,arg_table_row_number=0,arg_table_column_number=0,arg_color=0):
    #def csv_generator(self,output_file):
        #print(output_file)

        if (arg_ROI_code == 0):
            df = pd.DataFramedf = pd.DataFrame({'ocr_output':ocr_output, 'line_number':arg_line_number,'line_x': arg_line_x, 'line_y': arg_line_y,'line_height':arg_line_height,'line_width':arg_line_width,'font_size':arg_font_size,'orientationangle':arg_orientation})
            #print(arg_line_y)#arg_line_x,arg_line_y,arg_line_height)
            #df['uid']=[arg_uid] * len(df)
            df['docname']=[arg_docname] * len(df)
            df['pagenumber']=[arg_pagenumber] * len(df)
            df['pagelocation']=[arg_pagelocation] * len(df)
            df['Image_name']=[str_Image_name] * len(df)
            df['ROItopleftx'] = [arg_ROI_x]* len(df)
            df['ROItoplefty'] = [arg_ROI_y]* len(df)
            df['underline'] = [bool_underline]* len(df)
            #df['font_size'] = [arg_font_size]* len(df)
            df['table_row_number'] = [arg_table_row_number]* len(df)
            df['table_column_number'] = [arg_table_column_number]* len(df)
            df['color'] = [arg_color]* len(df)
            #df['line_height'] = [arg_line_height]* len(df)
            #df['line_width'] = [arg_line_width]* len(df)
            
        
            
        elif(arg_ROI_code == 1):
            df = pd.DataFrame({'ocr_output':ocr_output},index=ocr_output)
                
            df['Image_name']=[str_Image_name] * len(df)
            df['ROI_x'] = [arg_ROI_x]* len(df)
            df['ROI_y'] = [arg_ROI_y]* len(df)
            df['underline'] = [bool_underline]* len(df)
            df['font_size'] = [arg_font_size]* len(df)
            df['color'] = [arg_color]* len(df)
            df['line_height'] = [arg_line_height]* len(df)
            df['line_width'] = [arg_line_width]* len(df)
            df['line_x'] = [arg_line_x]* len(df)
            df['line_y'] = [arg_line_y]* len(df)
            df['table_row_number'] = [arg_table_row_number]* len(df)
            df['table_column_number'] = [arg_table_column_number]* len(df)
               
   
        
        else:
            df = pd.DataFrame({'ocr_output':ocr_output},index=ocr_output)
            df['Image_name']=[str_Image_name] * len(df)
            df['ROI_x'] = [arg_ROI_x]* len(df)
            df['ROI_y'] = [arg_ROI_y]* len(df)
            df['underline'] = [bool_underline]* len(df)
            df['font_size'] = [arg_font_size]* len(df)
            df['table_column_number'] = [arg_table_column_number == 0]* len(df)
            df['color'] = [arg_color]* len(df)
            df['line_height'] = [arg_line_height]* len(df)
            df['line_width'] = [arg_line_width]* len(df)
            df['line_x'] = [arg_line_x]* len(df)
            df['line_y'] = [arg_line_y]* len(df)
               #df['Pipeline_num'] = [arg_pipeline_num]* len(df)
        #checking whether the csv exists for the document

        #path1=output_file[0]
        if os.path.exists(path1):
            old_df = pd.read_csv(path1)
            new_df = old_df.append(df)
            new_df.to_csv(path1,index=False)
        # if it is not existing creating a new csv
        else:
            #df.to_csv(path1,index=False)
            print('we are in else')
            df.to_csv(path1,index=False)

        return (df)
        
    def csv_generator2(self,path1,arg_docname,arg_pagenumber,arg_pagelocation,ocr_output,arg_line_number,arg_line_x,arg_line_y,arg_line_height,arg_line_width,arg_font_size,str_Image_name,arg_ROI_x,arg_ROI_y,arg_pipeline_num,arg_orientation,bool_underline,arg_table_row_number,arg_table_column_number,arg_color):
        df = pd.DataFramedf = pd.DataFrame(
            {'ocr_output':ocr_output, 
            'line_number':arg_line_number,
            'line_x': arg_line_x, 
            'line_y': arg_line_y,
            'line_height':arg_line_height,
            'line_width':arg_line_width,
            'font_size':arg_font_size,
            'docname':arg_docname,
            'pagenumber':arg_pagenumber,
            'pagelocation':arg_pagelocation,
            'Image_name':str_Image_name,
            'underline':bool_underline,
            'table_row_number':arg_table_row_number,
            'table_column_number':arg_table_column_number,
            'color':arg_color,
            'pipelinename':arg_pipeline_num,
            'orientationangle':arg_orientation})
        return df

    

'''

   
'''