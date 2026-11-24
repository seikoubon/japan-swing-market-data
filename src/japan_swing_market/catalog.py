from dataclasses import dataclass


@dataclass(frozen=True)
class Dataset:
    endpoint: str
    date: str
    keys: tuple[str, ...]
    query_date: str = "date"
    numeric: tuple[str, ...] = ()
    required_daily: bool = False


OPTIONS_225 = "options_225"


DATASETS = {
    "master": Dataset("/equities/master", "Date", ("Date", "Code"), required_daily=True),
    "prices": Dataset(
        "/equities/bars/daily",
        "Date",
        ("Date", "Code"),
        numeric=("O", "H", "L", "C", "Vo", "Va", "AdjFactor", "AdjO", "AdjH", "AdjL", "AdjC", "AdjVo"),
        required_daily=True,
    ),
    "financials": Dataset("/fins/summary", "DiscDate", ("DiscDate", "Code", "DiscNo")),
    "calendar": Dataset("/markets/calendar", "Date", ("Date",), required_daily=True),
    "indices": Dataset(
        "/indices/bars/daily", "Date", ("Date", "Code"), numeric=("O", "H", "L", "C"), required_daily=True
    ),
    "margin": Dataset(
        "/markets/margin-interest",
        "Date",
        ("Date", "Code"),
        numeric=(
            "ShrtVol",
            "LongVol",
            "ShrtNegVol",
            "LongNegVol",
            "ShrtStdVol",
            "LongStdVol",
            "ShrtVal",
            "LongVal",
            "ShrtNegVal",
            "LongNegVal",
            "ShrtStdVal",
            "LongStdVal",
        ),
    ),
    "short_ratio": Dataset(
        "/markets/short-ratio",
        "Date",
        ("Date", "S33"),
        numeric=("SellExShortVa", "ShrtWithResVa", "ShrtNoResVa"),
    ),
    # Multiple reports by identical anonymous names can coexist: retain all distinct source rows.
    "short_sales": Dataset(
        "/markets/short-sale-report",
        "DiscDate",
        ("DiscDate", "_row_hash"),
        query_date="disc_date",
        numeric=("ShrtPosToSO", "ShrtPosShares", "ShrtPosUnits"),
    ),
    "margin_alert": Dataset(
        "/markets/margin-alert",
        "PubDate",
        ("PubDate", "Code", "AppDate"),
        numeric=("ShrtOut", "LongOut", "SLRatio", "ShrtNegOut", "ShrtStdOut", "LongNegOut", "LongStdOut"),
    ),
    "investor_types": Dataset(
        "/equities/investor-types", "PubDate", ("PubDate", "StDate", "EnDate", "Section")
    ),
    "earnings_dates": Dataset("/fins/earnings-date", "PubDate", ("PubDate", "Code", "FQName", "FYE")),
    "valuation": Dataset("/equities/valuation", "Date", ("Date", "Code")),
    OPTIONS_225: Dataset(
        "/derivatives/bars/daily/options/225",
        "Date",
        ("Date", "Code", "EmMrgnTrgDiv"),
        numeric=(
            "O", "H", "L", "C", "EO", "EH", "EL", "EC", "AO", "AH", "AL", "AC",
            "Vo", "OI", "Va", "Strike", "VoOA", "Settle", "Theo", "BaseVol", "UnderPx", "IV", "IR",
        ),
    ),
}
DEFAULT_DATASETS = tuple(name for name in DATASETS if name != OPTIONS_225)
INCREMENTAL_DATASETS = (*DEFAULT_DATASETS, OPTIONS_225)
OPTIONS_225_COLUMNS = {*DATASETS[OPTIONS_225].keys, *DATASETS[OPTIONS_225].numeric, "CM", "PCDiv", "LTD", "SQD"}
MARKETS = ("0111", "0112", "0113", "0101", "0102", "0104", "0106", "0107")
FIN_TEXT = {
    "DiscDate",
    "DiscTime",
    "Code",
    "DiscNo",
    "DocType",
    "CurPerType",
    "CurPerSt",
    "CurPerEn",
    "CurFYSt",
    "CurFYEn",
    "NxtFYSt",
    "NxtFYEn",
    "MatChgSub",
    "SigChgInC",
    "ChgByASRev",
    "ChgNoASRev",
    "ChgAcEst",
    "RetroRst",
}


def is_numeric(dataset, field):
    if dataset == "financials":
        return field not in FIN_TEXT and not field.startswith("_")
    if dataset == "investor_types":
        return field not in {"PubDate", "StDate", "EnDate", "Section"} and not field.startswith("_")
    if dataset == "valuation":
        return field not in {"Date", "Code"} and not field.startswith("_")
    return field in DATASETS[dataset].numeric
