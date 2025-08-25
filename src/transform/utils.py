from typing import Dict, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Union, List

from pyspark.sql.functions import lit


def preprocessing(dataframe,
                  technical_keys: Dict[str, List[str]] = {},
                  filter_on=None,
                  audit_columns: Optional[Dict[str, str]] = None):
    dataframe = create_audit_columns(dataframe, audit_columns)
    dataframe = create_technical_column(dataframe, technical_keys=technical_keys)
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

    # null ou vide après trim
    def is_blank(c):
        return F.col(c).isNull() | (F.length(F.trim(F.col(c))) == 0)

    sanitized = [F.when(is_blank(c), F.lit(None)).otherwise(F.col(c)) for c in columns]

    dataframe = dataframe.withColumn(
        tech_col_name, F.md5(F.concat_ws("^", *sanitized)))

    # Réordonner les colonnes pour que "id tech" soit en premier
    cols = [tech_col_name] + [c for c in dataframe.columns if c != tech_col_name]
    dataframe = dataframe.select(cols)
    print("Technical_key_col created !")
    return dataframe


def replace_nulls_values(dataframe, columns_to_clean: Dict[str, str]):

    columns_to_clean= list(columns_to_clean.values())[0]
    print(columns_to_clean, type(columns_to_clean),"columns_to_clean")
    columns_to_clean= columns_to_clean if  columns_to_clean else {}

    dataframe = dataframe.na.fill(columns_to_clean)
    return dataframe


def create_technical_column(dataframe, technical_keys: Dict[str, List[str]]):
    """
    this function take a dataframe and dict of technical keys to generate
    """
    print(technical_keys)

    for technical_keys, columnsDict in technical_keys.items():
        for tech_col_name, columns in columnsDict.items():
            print(columns)
        dataframe = create_technical_key_col(dataframe, tech_col_name=tech_col_name, columns=columns)
        return dataframe
    return None




def clean_types_columns(df: DataFrame, columns_to_clean: Union[str, List[str]]) -> DataFrame:
    """
    Remplace les valeurs de type TRANSACTION_TYPE.SELL par SELL dans une ou plusieurs colonnes.

    Args:
        df (DataFrame): DataFrame PySpark
        cols (str | List[str]): Nom d'une colonne ou liste de colonnes à nettoyer

    Returns:
        DataFrame: DataFrame avec colonnes nettoyées
        :param columns_to_clean:
    """
    if isinstance(columns_to_clean, str):
        columns_to_clean = [columns_to_clean]

    columns_to_clean= columns_to_clean.get("columns_to_clean", [])


    for col in columns_to_clean:
        # on coupe sur le "." et on prend la dernière partie
        df = df.withColumn(
            col,
            F.when(
                F.col(col).rlike(r"^(ITEM_TYPE|TRANSACTION_TYPE)\..+"),
                F.substring_index(F.col(col), ".", -1)
            ).otherwise(lit('unknown'))
        )

    return df
