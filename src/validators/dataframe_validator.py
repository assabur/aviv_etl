import json
import os
import uuid
from datetime import datetime, timezone
from dotenv import load_dotenv
from great_expectations.dataset import SparkDFDataset
from typing import List, Union, Callable, Optional, Dict, Any
from src.validators import AbstractValidator



class CoreValidator(AbstractValidator):
    def __init__(self,
        columns: Union[str, List[str]],rule,dataset_name, output_table_name: str,rule_kwargs: Optional[Dict[str, Any]] = None,) -> None:
        self.columns= columns
        self.rule= rule
        self.output_table_name= output_table_name
        self.rule_kwargs= rule_kwargs
        self.dataset_name= dataset_name


    def validate(self, dataframe, spark) -> None:
        self.run_ge_rule_to_ndjson (dataframe=dataframe,rule=self.rule,columns=self.columns,
                                    dataset_name=self.dataset_name,rule_kwargs=self.rule_kwargs,
                                    output_table_name=self.output_table_name)


    # Utilitaire : aplatit un résultat GE en "event" ELK-friendly

    def _ge_results_to_events(self,validation: dict,
                              dataset_name: str,
                              run_id: str,
                              ts_iso: str) -> List[dict]:
        events = []
        for r in validation.get("results", []):
            cfg = r.get("expectation_config", {}) or {}
            res = r.get("result", {}) or {}
            kwargs = cfg.get("kwargs", {}) or {}
            events.append({
                "@timestamp": ts_iso,
                "run_id": run_id,
                "dataset": dataset_name,
                "success_overall": validation.get("success"),
                "expectation_type": cfg.get("expectation_type"),
                "column": kwargs.get("column"),
                "success": r.get("success"),
                "unexpected_count": res.get("unexpected_count"),
                "unexpected_percent": res.get("unexpected_percent"),
                "element_count": res.get("element_count"),

                "partial_unexpected_list": res.get("partial_unexpected_list"),
                "partial_unexpected_counts": res.get("partial_unexpected_counts"),
            })
        return events


    def run_ge_rule_to_ndjson(self,
            dataframe, *,
            rule: Union[str, Callable[..., dict]],
            columns: Optional[Union[str, List[str]]] = None,
            rule_kwargs: Optional[Dict[str, Any]] = None,
            dataset_name: str = "ge_validation",
            output_dir: str = "/ge_logs/",
            output_table_name: Optional[str] = None,
            result_format: str = "COMPLETE",
    ) -> str:
        """
        Applique une règle Great Expectations (rule) à un DataFrame Spark et écrit le résultat en NDJSON.

        Args:
            dataframe: Spark DataFrame source.
            rule:
              - Nom d'expectation GE (str), ex: "expect_column_values_to_be_unique"
              - ou callable(df_ge, **kwargs) retournant un dict de résultat (pour règles custom).
            columns:
              - None => règle "dataset-level" (pas de colonne)
              - str  => une seule colonne
              - List[str] => applique la règle sur chaque colonne
            rule_kwargs: kwargs passés à la règle (ex: {"regex": r"^\\d{5}$"}).
            dataset_name: nom logique pour vos dashboards/ELK.
            output_dir: répertoire de sortie NDJSON.
            output_table_name: sous-dossier logique (ex: nom de table).
            result_format: "BASIC" | "SUMMARY" | "COMPLETE".
        Returns:
            Chemin du fichier NDJSON écrit.
        """
        rule_kwargs = rule_kwargs or {}

        # IDs de run
        run_id = str(uuid.uuid4())
        ts_iso = datetime.now(timezone.utc).isoformat()

        # Dataset GE
        df_ge = SparkDFDataset(dataframe)

        # Helper pour appeler la règle
        def _apply_rule_on_col(col: Optional[str] = None):
            if callable(rule):
                # Règle custom fournie en callable
                return rule(df_ge, **({**rule_kwargs, **({"column": col} if col else {})}))
            else:
                # Règle GE par son nom (string)
                fn = getattr(df_ge, rule, None)
                if fn is None:
                    raise ValueError(f"Expectation '{rule}' introuvable sur SparkDFDataset")
                if col is None:
                    return fn(**rule_kwargs)
                else:
                    return fn(column=col, **rule_kwargs)

        # Appliquer la/les expectations (on remplit la suite d'expectations du dataset)
        if columns is None:
            _apply_rule_on_col(None)
        elif isinstance(columns, str):
            _apply_rule_on_col(columns)
        else:
            for col in columns:
                _apply_rule_on_col(col)

        # Valider
        validation = df_ge.validate(result_format=result_format)

        # Transformer en events plats
        events = self._ge_results_to_events(validation, dataset_name, run_id, ts_iso)

        # Sortie NDJSON
        load_dotenv()
        silver = os.getenv("SILVER")
        out_dir = os.path.join(silver+output_dir+dataset_name, output_table_name) if output_table_name else output_dir
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"ge_validation-{run_id}.ndjson")

        with open(out_path, "w", encoding="utf-8") as f:
            for e in events:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")

        return out_path
