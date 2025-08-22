
from typing import Dict, List, Optional
import pyspark.sql.functions as F



def preprocessing(dataframe,
                  technical_keys: Dict[str, List[str]] = {},
                  filter_on=None,
                  audit_columns: Optional[Dict[str, str]] = None):

    dataframe = create_audit_columns(dataframe, audit_columns)
    dataframe = create_technical_column(dataframe, technical_keys=technical_keys)
    dataframe.show()
    return dataframe if filter_on is None else dataframe.filter(filter_on)





def create_audit_columns(dataframe, audit_columns: Dict[str, str]):
    if audit_columns is None:
        return dataframe
    for key, value in audit_columns.items():
        dataframe = dataframe.withColumn(key, F.expr(value))
    return dataframe


def create_technical_key_col(dataframe, tech_col_name: str, columns: List[str]):
    """
    Generate the technicals key cols by using :
    the MD5 hash function
    with item concat with "^"
    Take care about the type of cols
    For instance, integer or bigint having the same value are not stored on the
    same numbers of bytes why the hash will be different
    """
    print ("in create_technical_key_col")

    # null ou vide après trim
    def is_blank(c):
        return F.col(c).isNull() | (F.length(F.trim(F.col(c))) == 0)

    sanitized = [F.when(is_blank(c), F.lit(None)).otherwise(F.col(c)) for c in columns]
    any_blank = F.reduce(lambda a, b: a | b, [is_blank(c) for c in columns])

    print ("sanitized", sanitized)
    print ("any_blank", any_blank)

    dataframe = dataframe.withColumn(
        tech_col_name,
        F.when(any_blank, F.lit(None))  # -> None si au moins un champ est vide
        .otherwise(F.md5(F.concat_ws("^", *sanitized)))  # md5 = STRING (32 hex)
    )
    dataframe.show()
    return dataframe


def create_technical_column(dataframe, technical_keys: Dict[str, List[str]]):
    """
    this function take a dataframe and dict of technical keys to generate
    """
    print(technical_keys)

    for technical_keys, columnsDict in technical_keys.items():
        for tech_col_name,columns in columnsDict.items():
            print (columns)
        dataframe = create_technical_key_col(dataframe, tech_col_name=tech_col_name, columns=columns)
        return dataframe
    return None



