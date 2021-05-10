# This module is used for generic transformations that are reused across



def renameColumn(df, oldName, newName):
    df_renamed = df.withColumnRenamed(oldName, newName)
    return df_renamed
   
def filterdf(df, column, country):
    df_filter = df.filter(column == country)
    return df_filter  