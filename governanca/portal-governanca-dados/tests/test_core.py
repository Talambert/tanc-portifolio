import re

import pandas as pd

from governanca.core import masking
from governanca.mock import data as mock


def test_mask_text_hides_email_cpf_cnpj_and_phone():
    text = "ana@exemplo.com 123.456.789-00 12.345.678/0001-90 (11) 91234-5678"
    masked = masking.mask_text(text)
    for secret in ("ana@exemplo.com", "123.456.789-00", "12.345.678/0001-90", "91234-5678"):
        assert secret not in masked


def test_mask_dataframe_hides_sensitive_columns_by_name():
    df = pd.DataFrame({"cpf": ["123.456.789-00"], "email": ["ana@exemplo.com"], "uf": ["SP"]})
    masked = masking.mask_dataframe(df)
    assert masked.loc[0, "cpf"] == masking.MASK
    assert masked.loc[0, "email"] == masking.MASK
    assert masked.loc[0, "uf"] == "SP"


def test_mask_dataframe_does_not_alter_original():
    df = pd.DataFrame({"cpf": ["123.456.789-00"]})
    masking.mask_dataframe(df)
    assert df.loc[0, "cpf"] == "123.456.789-00"


def test_no_sensitive_value_survives_in_any_masked_sample():
    """Nenhuma amostra mascarada pode conter e-mail ou CPF em texto claro."""
    for dataset in mock.list_datasets():
        for table in mock.list_tables(dataset):
            masked = masking.mask_dataframe(mock.get_sample(dataset, table, dirty=True))
            flat = masked.to_csv(index=False)
            assert not re.search(r"[\w.+-]+@[\w-]+\.[\w.-]+", flat), f"e-mail em {dataset}.{table}"
            assert not re.search(r"\d{3}\.\d{3}\.\d{3}-\d{2}", flat), f"CPF em {dataset}.{table}"


def test_every_table_has_samples_and_suggestions():
    for dataset in mock.list_datasets():
        for table in mock.list_tables(dataset):
            assert not mock.get_sample(dataset, table).empty
            assert (dataset, table) in mock.TABLE_SUGGESTIONS
            declared = {c[0] for c in mock.DATASETS[dataset][table]["columns"]}
            assert declared == set(mock.COLUMN_SUGGESTIONS[(dataset, table)])
