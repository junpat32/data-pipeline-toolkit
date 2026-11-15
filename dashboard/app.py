import argparse
import json
from datetime import date, datetime
from decimal import Decimal, localcontext
from pathlib import Path

import pandas as pd
import streamlit as st


VALUE_COLUMNS = ("gross_sales_value", "negative_line_value", "net_line_value")


def read_table(directory, filename):
    table = pd.read_csv(
        directory / filename, keep_default_na=False,
        dtype={"country": str, "stock_code": str, "delivery_id": str},
        converters={column: Decimal for column in VALUE_COLUMNS},
    )
    if "day" in table:
        table["day"] = table["day"].map(date.fromisoformat)
    for column in ("country", "stock_code"):
        if column in table:
            # The exporter prefixes formula-like and leading-apostrophe strings.
            table[column] = table[column].str.removeprefix("'")
    return table


def load_package(directory):
    if directory.resolve().name.startswith(".staging-") or not (directory / "_SUCCESS").is_file():
        raise ValueError("Choose a completed report directory containing _SUCCESS.")
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    if manifest["dataset"] != "retail" or manifest["schema_version"] != 1:
        raise ValueError("Choose a version 1 retail report package.")
    return (
        manifest,
        read_table(directory, "retail_daily.csv"),
        read_table(directory, "retail_products.csv"),
        read_table(directory, "findings_by_rule.csv"),
    )


def select_rows(table, start, end, countries):
    return table[table["day"].between(start, end) & table["country"].isin(countries)]


def gbp(value):
    return f"£{value:,.2f}"


def show_retail(daily, products):
    gross, negative, net = (daily[column].sum() for column in VALUE_COLUMNS)
    ratio = f"{negative / gross:.2%}" if gross else "Unavailable"
    for column, label, value in zip(
        st.columns(4),
        ("Gross sales", "Negative-line value", "Net line value", "Negative-line value ratio"),
        (gbp(gross), gbp(negative), gbp(net), ratio),
        strict=True,
    ):
        column.metric(label, value, border=True)
    if daily.empty:
        st.info("No accepted lines match these filters.")
        return
    st.caption(
        f"{daily['sales_lines'].sum():,} sales lines · "
        f"{daily['negative_quantity_lines'].sum():,} negative-quantity lines (returns proxy) · "
        f"{daily['cancellation_lines'].sum():,} cancellation-flagged lines"
    )

    trend, ranked = st.columns([3, 2])
    with trend:
        st.subheader("Daily value")
        trends = daily.groupby("day")[list(VALUE_COLUMNS)].sum().astype(float)
        trends.index = trends.index.map(date.isoformat)
        trends.index.name = "Date"
        trends.columns = ["Gross sales", "Negative-line value", "Net line value"]
        st.bar_chart(trends, color=["#147C73", "#B7653C", "#344F74"], y_label="GBP", stack=False)
    with ranked:
        st.subheader("Products by negative-line value")
        ranking = products.groupby("stock_code", as_index=False)[
            ["negative_line_value", "negative_quantity_lines"]
        ].sum()
        ranking = ranking[ranking["negative_line_value"] > 0].sort_values(
            ["negative_line_value", "stock_code"], ascending=[False, True],
        )
        if ranking.empty:
            st.caption("No products have negative-line value in this selection.")
        else:
            ranking["negative_line_value"] = ranking["negative_line_value"].map(gbp)
            st.dataframe(ranking.head(20), hide_index=True, width="stretch", column_config={
                "stock_code": "Product",
                "negative_line_value": "Negative-line value",
                "negative_quantity_lines": "Negative-quantity lines",
            })

    uk_lines = daily["uk_lines"].sum()
    uk_with_id = daily["uk_lines_with_id"].sum()
    coverage = f"{uk_with_id / uk_lines:.1%}" if uk_lines else "Unavailable"
    st.markdown(f"**UK customer ID coverage** · {coverage} ({uk_with_id:,} of {uk_lines:,} UK lines in this selection)")


def show_quality(manifest, findings):
    st.subheader("Delivery quality")
    st.caption("Counts cover all included deliveries and dates in this package.")
    deliveries = pd.DataFrame(manifest["deliveries"])
    fields = {
        "input_rows": "Input", "accepted_rows": "Accepted", "rejected_rows": "Rejected",
        "warned_rows": "Warned", "finding_count": "Findings",
    }
    for column, (field, label) in zip(st.columns(len(fields)), fields.items(), strict=True):
        column.metric(label, f"{deliveries[field].sum():,}", border=True)
    import_table, rule_table = st.columns([3, 2])
    with import_table:
        st.dataframe(
            deliveries[["delivery_id", *fields]].rename(columns={"delivery_id": "Delivery", **fields}),
            hide_index=True, width="stretch",
        )
    with rule_table:
        if findings.empty:
            st.caption("No findings in the included deliveries.")
        else:
            st.dataframe(findings, hide_index=True, width="stretch", column_config={
                "severity": "Severity", "check_code": "Rule", "field": "Field", "findings": "Findings",
            })


def main():
    parser = argparse.ArgumentParser(description="Open a completed retail report package.")
    parser.add_argument("--report-dir", required=True, type=Path)
    args = parser.parse_args()
    st.set_page_config(page_title="Retail sales and returns", layout="wide")
    st.title("Retail sales and returns")
    try:
        manifest, daily, products, findings = load_package(args.report_dir)
        first = date.fromisoformat(manifest["date_scope"]["start"])
        last = date.fromisoformat(manifest["date_scope"]["end"])
        generated = datetime.fromisoformat(manifest["generated_at"])
    except (OSError, ValueError, KeyError, TypeError, ArithmeticError) as error:
        st.error(f"Could not open the retail report package. {error}")
        return
    delivery_ids = ", ".join(item["delivery_id"] for item in manifest["deliveries"])
    st.caption(
        f"Deliveries {delivery_ids} · Package dates {first:%d %b %Y} to {last:%d %b %Y} "
        f"· Published {generated:%Y-%m-%d %H:%M:%S} UTC"
    )
    date_filter, country_filter = st.columns([1, 2])
    selected_dates = date_filter.date_input(
        "Date range", (first, last), min_value=first, max_value=last, format="YYYY-MM-DD",
    )
    countries = sorted(daily["country"].unique())
    selected_countries = country_filter.multiselect("Countries", countries, default=countries)
    if len(selected_dates) == 2:
        start, end = selected_dates
        with localcontext() as context:
            context.prec = 48
            show_retail(
                select_rows(daily, start, end, selected_countries),
                select_rows(products, start, end, selected_countries),
            )
    else:
        st.info("Choose both a start and end date.")
    st.divider()
    show_quality(manifest, findings)

    with st.expander("Metric definitions"):
        st.markdown(
            "- Line amount is quantity × unit price in GBP. Gross sales sums positive line amounts. "
            "Negative-line value is the absolute sum of negative line amounts. Net value is the signed sum.\n"
            "- The value ratio divides negative-line value by gross sales. It is unavailable when gross sales is zero.\n"
            "- Positive quantities count as sales lines. Negative quantities are a returns proxy. "
            "Invoice prefixes C and c flag cancellations independently of quantity.\n"
            "- The recorded timestamps have no source timezone offset. UK customer ID coverage uses all accepted "
            "UK lines in the current selection as its denominator."
        )


if __name__ == "__main__":
    main()
