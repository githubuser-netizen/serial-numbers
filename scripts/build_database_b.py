#!/usr/bin/env python3
"""Rebuild Database B with normalized customer IDs and Supabase-ready exports."""
from __future__ import annotations

import csv
import math
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
import zipfile
from xml.etree import ElementTree as ET

NS_MAIN = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
NS_REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
NS_PKG_REL = {"rel": "http://schemas.openxmlformats.org/package/2006/relationships"}

EXCEL_DATE_BASE = datetime(1899, 12, 30)

HEADER_OVERRIDES = {
    "column_04": "customer_tags",
    "column_12": "internal_note_1",
    "column_13": "internal_note_2",
    "column_17": "fulfillment_contact",
    "column_29": "number_text_primary",
    "column_30": "number_text_secondary",
    "column_49": "social_contact_name",
    "column_50": "social_contact_handle",
    "column_51": "extra_column_51",
    "column_52": "extra_column_52",
    "column_53": "extra_column_53",
    "column_54": "extra_column_54",
    "column_55": "extra_column_55",
    "column_56": "extra_column_56",
    "column_57": "extra_column_57",
}


@dataclass
class Record:
    row: List[str]
    fields: Dict[str, str]
    date: Optional[datetime]
    settlement_date: Optional[datetime]
    name_norm: str
    phone_norm: str
    serial_norm: str
    customer_key: str
    source: str


@dataclass
class MatchLogEntry:
    customer_key: str
    source: str
    match_type: str
    record_name: str
    record_phone: str
    record_serial: str
    record_date: Optional[datetime]
    matched_key: Optional[str]
    matched_name: str
    matched_phone: str


@dataclass
class ManualReviewEntry:
    customer_key: str
    match_type: str
    record_name: str
    record_phone: str
    matched_name: str
    matched_phone: str
    reason: str


def col_to_index(col: str) -> int:
    idx = 0
    for ch in col.upper():
        idx = idx * 26 + (ord(ch) - 64)
    return idx - 1


def load_sheet(path: Path, sheet_name: str) -> List[List[str]]:
    with zipfile.ZipFile(path) as zf:
        shared_strings: List[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in root.findall("main:si", NS_MAIN):
                text_parts: List[str] = []
                t = si.find("main:t", NS_MAIN)
                if t is not None:
                    text_parts.append(t.text or "")
                else:
                    for run in si.findall("main:r", NS_MAIN):
                        rt = run.find("main:t", NS_MAIN)
                        if rt is not None:
                            text_parts.append(rt.text or "")
                shared_strings.append("".join(text_parts))

        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        sheets = workbook.find("main:sheets", NS_MAIN)
        if sheets is None:
            raise ValueError("Workbook missing sheets definition")

        rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        rel_map = {
            rel.attrib["Id"]: rel.attrib["Target"]
            for rel in rels.findall("rel:Relationship", NS_PKG_REL)
        }

        for sheet in sheets.findall("main:sheet", NS_MAIN):
            if sheet.attrib.get("name") != sheet_name:
                continue
            rid = sheet.attrib.get(f"{{{NS_REL}}}id")
            if not rid:
                raise ValueError(f"Sheet {sheet_name} missing relationship id")
            target = rel_map[rid]
            if not target.startswith("xl/"):
                target = f"xl/{target}"
            tree = ET.fromstring(zf.read(target))
            sheet_data = tree.find("main:sheetData", NS_MAIN)
            if sheet_data is None:
                return []

            rows: List[Dict[int, str]] = []
            max_col = -1
            for row in sheet_data.findall("main:row", NS_MAIN):
                row_cells: Dict[int, str] = {}
                for cell in row.findall("main:c", NS_MAIN):
                    ref = cell.attrib.get("r", "")
                    match = re.match(r"([A-Z]+)", ref)
                    if match:
                        idx = col_to_index(match.group(1))
                    else:
                        idx = len(row_cells)

                    cell_type = cell.attrib.get("t")
                    value = ""
                    v = cell.find("main:v", NS_MAIN)
                    if cell_type == "s" and v is not None:
                        value = shared_strings[int(v.text)]
                    elif v is not None:
                        value = v.text or ""
                    is_elem = cell.find("main:is", NS_MAIN)
                    if is_elem is not None:
                        t_elem = is_elem.find("main:t", NS_MAIN)
                        if t_elem is not None:
                            value = t_elem.text or ""

                    row_cells[idx] = value
                    if idx > max_col:
                        max_col = idx
                rows.append(row_cells)

            width = max_col + 1
            data: List[List[str]] = []
            for row_cells in rows:
                row_list = [""] * width
                for idx, value in row_cells.items():
                    row_list[idx] = value
                data.append(row_list)
            return data

    raise ValueError(f"Sheet {sheet_name!r} not found in {path}")


def to_snake_case(name: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z]+", "_", name).strip("_")
    return cleaned.lower()


def sanitize_headers(headers: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    result: List[str] = []
    for idx, header in enumerate(headers):
        base = header.strip()
        if not base:
            base = f"column_{idx+1:02d}"
        snake = to_snake_case(base)
        if not snake:
            snake = f"column_{idx+1:02d}"
        count = seen.get(snake, 0)
        if count:
            snake = f"{snake}_{count+1}"
        seen[snake] = count + 1
        result.append(snake)
    return result


def apply_header_overrides(headers: List[str]) -> List[str]:
    return [HEADER_OVERRIDES.get(name, name) for name in headers]


def parse_excel_date(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        number = float(value)
        if math.isnan(number):
            return None
        return EXCEL_DATE_BASE + timedelta(days=number)
    except ValueError:
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d %b %Y"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
    return None


def format_date(value: Optional[datetime]) -> str:
    return value.strftime("%Y-%m-%d") if value else ""


def normalize_phone(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def normalize_name(value: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z]+", " ", value or "").strip().lower()
    return re.sub(r"\s+", " ", cleaned)


def normalize_serial(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "", (value or "").upper())


def make_customer_key(phone_norm: str, name_norm: str, fallback: List[int]) -> str:
    if phone_norm:
        return f"phone:{phone_norm}"
    if name_norm:
        return f"name:{name_norm}"
    fallback[0] += 1
    return f"anon:{fallback[0]}"


def build_record(
    row: List[str],
    index_map: Dict[str, int],
    fallback: List[int],
    source: str,
    existing_key: Optional[str] = None,
) -> Record:
    fields = {name: row[idx] for name, idx in index_map.items() if idx < len(row)}
    date = parse_excel_date(fields.get("date", ""))
    settlement = parse_excel_date(fields.get("actual_settlement_date", ""))
    name_norm = normalize_name(fields.get("name", ""))
    phone_norm = normalize_phone(fields.get("number", ""))
    serial_norm = normalize_serial(fields.get("serial_number", ""))
    customer_key = existing_key or make_customer_key(phone_norm, name_norm, fallback)
    return Record(
        row=row,
        fields=fields,
        date=date,
        settlement_date=settlement,
        name_norm=name_norm,
        phone_norm=phone_norm,
        serial_norm=serial_norm,
        customer_key=customer_key,
        source=source,
    )


def update_row_dates(record: Record, index_map: Dict[str, int]) -> None:
    date_idx = index_map.get("date")
    if date_idx is not None and date_idx < len(record.row):
        record.row[date_idx] = format_date(record.date)
    settlement_idx = index_map.get("actual_settlement_date")
    if settlement_idx is not None and settlement_idx < len(record.row):
        record.row[settlement_idx] = format_date(record.settlement_date)


def main(argv: Iterable[str]) -> None:
    base_dir = Path.cwd()
    b_path = base_dir / "Backup Copy of CPAP Stock Records and Serial Numbers 28 Aug 2025 (5).xlsx"
    a_path = base_dir / "Database A - CPAP Stock Records and Serial Numbers.xlsx"
    output_dir = base_dir / "supabase"
    output_dir.mkdir(exist_ok=True)

    if "(5)" not in b_path.name:
        raise SystemExit(
            "Refusing to proceed: expected Database B workbook with version '(5)' in the title."
        )

    sheet_b = load_sheet(b_path, "Outgoing Serial Numbers")
    if not sheet_b:
        raise SystemExit("Database B sheet is empty")
    headers = apply_header_overrides(sanitize_headers(sheet_b[0]))
    width = len(headers)

    rows_b: List[List[str]] = []
    for row in sheet_b[1:]:
        if len(row) < width:
            row = row + [""] * (width - len(row))
        if any(cell for cell in row):
            rows_b.append(row)

    index_map = {name: idx for idx, name in enumerate(headers)}

    fallback_counter = [0]
    records: List[Record] = []
    serial_index: Dict[str, Record] = {}
    phone_index: Dict[str, Record] = {}
    name_index: Dict[str, Record] = {}
    match_logs: List[MatchLogEntry] = []
    manual_review: List[ManualReviewEntry] = []

    for row in rows_b:
        record = build_record(row, index_map, fallback_counter, source="existing")
        update_row_dates(record, index_map)
        records.append(record)
        if record.serial_norm and record.serial_norm not in serial_index:
            serial_index[record.serial_norm] = record
        if record.phone_norm and record.phone_norm not in phone_index:
            phone_index[record.phone_norm] = record
        if record.name_norm and record.name_norm not in name_index:
            name_index[record.name_norm] = record

    sheet_a = load_sheet(a_path, "Outgoing Serial Numbers")
    if not sheet_a:
        raise SystemExit("Database A sheet is empty")
    headers_a = apply_header_overrides(sanitize_headers(sheet_a[0]))
    index_map_a = {name: idx for idx, name in enumerate(headers_a)}

    cutoff = datetime(2025, 8, 28)
    added_rows = 0
    for row in sheet_a[1:]:
        if not any(row):
            continue
        if len(row) < len(headers_a):
            row = row + [""] * (len(headers_a) - len(row))
        date_idx = index_map_a.get("date")
        date_value = row[date_idx] if date_idx is not None else ""
        parsed_date = parse_excel_date(date_value) if date_value else None
        if not parsed_date or parsed_date < cutoff:
            continue

        serial_idx_a = index_map_a.get("serial_number")
        serial_value = row[serial_idx_a] if serial_idx_a is not None else ""
        serial_norm = normalize_serial(serial_value)
        if serial_norm and serial_norm in serial_index:
            continue

        phone_idx_a = index_map_a.get("number")
        phone_value = row[phone_idx_a] if phone_idx_a is not None else ""
        phone_norm = normalize_phone(phone_value)
        name_idx_a = index_map_a.get("name")
        name_value = row[name_idx_a] if name_idx_a is not None else ""
        name_norm = normalize_name(name_value)

        existing_key: Optional[str] = None
        match_type = "new"
        matched_record: Optional[Record] = None
        if serial_norm and serial_norm in serial_index:
            matched_record = serial_index[serial_norm]
            existing_key = matched_record.customer_key
            match_type = "serial"
        elif phone_norm and phone_norm in phone_index:
            matched_record = phone_index[phone_norm]
            existing_key = matched_record.customer_key
            match_type = "phone"
        elif name_norm and name_norm in name_index:
            matched_record = name_index[name_norm]
            existing_key = matched_record.customer_key
            match_type = "exact_name"
        else:
            partial_match: Optional[Record] = None
            for existing_name, rec in name_index.items():
                if name_norm and (name_norm in existing_name or existing_name in name_norm):
                    partial_match = rec
                    break
            if partial_match is not None:
                matched_record = partial_match
                existing_key = matched_record.customer_key
                match_type = "partial_name"

        new_row = [""] * width
        for name, idx in index_map_a.items():
            if name in index_map and idx < len(row):
                new_row[index_map[name]] = row[idx]

        record = build_record(new_row, index_map, fallback_counter, source="database_a", existing_key=existing_key)
        update_row_dates(record, index_map)
        if record.source == "database_a":
            matched_name = matched_record.fields.get("name", "") if matched_record else ""
            matched_phone_norm = matched_record.phone_norm if matched_record else ""
            match_logs.append(
                MatchLogEntry(
                    customer_key=record.customer_key,
                    source=record.source,
                    match_type=match_type,
                    record_name=record.fields.get("name", ""),
                    record_phone=record.phone_norm,
                    record_serial=record.serial_norm,
                    record_date=record.date,
                    matched_key=matched_record.customer_key if matched_record else None,
                    matched_name=matched_name,
                    matched_phone=matched_phone_norm,
                )
            )
            if matched_record and match_type in {"exact_name", "partial_name"}:
                existing_phone = matched_record.phone_norm
                new_phone = record.phone_norm
                if existing_phone != new_phone:
                    if existing_phone and new_phone:
                        reason = "Name match but phone numbers differ"
                    elif existing_phone and not new_phone:
                        reason = "Name match; new transaction missing phone number"
                    elif new_phone and not existing_phone:
                        reason = "Name match; existing record missing phone number"
                    else:
                        reason = "Name match with undefined phone numbers"
                    manual_review.append(
                        ManualReviewEntry(
                            customer_key=record.customer_key,
                            match_type=match_type,
                            record_name=record.fields.get("name", ""),
                            record_phone=new_phone,
                            matched_name=matched_name,
                            matched_phone=existing_phone,
                            reason=reason,
                        )
                    )
        records.append(record)
        if record.serial_norm and record.serial_norm not in serial_index:
            serial_index[record.serial_norm] = record
        if record.phone_norm and record.phone_norm not in phone_index:
            phone_index[record.phone_norm] = record
        if record.name_norm and record.name_norm not in name_index:
            name_index[record.name_norm] = record
        added_rows += 1

    print(f"Added {added_rows} new transactions from Database A >= 28 Aug 2025")

    customers: Dict[str, List[Record]] = defaultdict(list)
    for record in records:
        customers[record.customer_key].append(record)

    def earliest_key_info(key: str) -> Tuple[datetime, str]:
        recs = customers[key]
        dates = [rec.date for rec in recs if rec.date]
        first_date = min(dates) if dates else datetime.max
        name = recs[0].name_norm
        return first_date, name

    sorted_keys = sorted(
        customers.keys(),
        key=lambda key: (earliest_key_info(key)[0], earliest_key_info(key)[1], key),
    )

    customer_id_map: Dict[str, str] = {
        key: f"CUST{idx:05d}"
        for idx, key in enumerate(sorted_keys, start=1)
    }

    customer_id_idx = index_map.get("customer_id")
    for key, recs in customers.items():
        customer_id = customer_id_map[key]
        for record in recs:
            record.fields["customer_id"] = customer_id
            if customer_id_idx is not None and customer_id_idx < len(record.row):
                record.row[customer_id_idx] = customer_id

    records.sort(
        key=lambda rec: (
            customer_id_map[rec.customer_key],
            rec.date or datetime.max,
            rec.serial_norm,
        )
    )

    transactions_path = output_dir / "database_b_transactions.csv"
    with transactions_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for record in records:
            row = record.row
            if len(row) < width:
                row = row + [""] * (width - len(row))
            writer.writerow(row[:width])

    customer_headers = [
        "customer_id",
        "primary_name",
        "first_name",
        "first_transaction_date",
        "most_recent_transaction_date",
        "transaction_count",
        "primary_phone",
        "alternate_phones",
        "email",
        "billing_address",
        "shipping_address",
        "district",
        "legacy_info",
        "sold_from",
        "handler",
        "remarks",
        "referrer",
        "failed_trial",
        "unreasonable_customer",
        "wealth_and_network_estimate",
        "serial_numbers",
        "items",
        "source_count_existing",
        "source_count_database_a",
    ]

    customer_rows: List[List[str]] = []
    for key in sorted_keys:
        recs = customers[key]
        customer_id = customer_id_map[key]
        recs_sorted = sorted(recs, key=lambda r: r.date or datetime.max)
        first_record = recs_sorted[0]
        primary_name = first_record.fields.get("name", "")
        first_name = first_record.fields.get("first_name", "")
        first_date = min((rec.date for rec in recs if rec.date), default=None)
        last_date = max((rec.date for rec in recs if rec.date), default=None)
        transaction_count = len(recs)
        primary_phone = first_record.fields.get("number", "")
        alt_phones = {
            rec.fields.get("number_text_primary", "")
            for rec in recs
            if rec.fields.get("number_text_primary")
        }
        alt_phones.update(
            rec.fields.get("number_text_secondary", "")
            for rec in recs
            if rec.fields.get("number_text_secondary")
        )
        alt_phones.discard(primary_phone)
        email = next((rec.fields.get("email_address", "") for rec in recs if rec.fields.get("email_address")), "")
        billing = next((rec.fields.get("billing_address", "") for rec in recs if rec.fields.get("billing_address")), "")
        shipping = next((rec.fields.get("shipping_address", "") for rec in recs if rec.fields.get("shipping_address")), "")
        district = next((rec.fields.get("district", "") for rec in recs if rec.fields.get("district")), "")
        legacy_info = next((rec.fields.get("legacy_info", "") for rec in recs if rec.fields.get("legacy_info")), "")
        sold_from = next((rec.fields.get("sold_from", "") for rec in recs if rec.fields.get("sold_from")), "")
        handler = next((rec.fields.get("handler", "") for rec in recs if rec.fields.get("handler")), "")
        remarks = {
            rec.fields.get("remarks", "")
            for rec in recs
            if rec.fields.get("remarks")
        }
        referrer = next((rec.fields.get("referrer", "") for rec in recs if rec.fields.get("referrer")), "")
        failed_trial = next((rec.fields.get("failed_trial", "") for rec in recs if rec.fields.get("failed_trial")), "")
        unreasonable = next((rec.fields.get("unreasonable_customer", "") for rec in recs if rec.fields.get("unreasonable_customer")), "")
        wealth = next((rec.fields.get("wealth_and_network_estimate", "") for rec in recs if rec.fields.get("wealth_and_network_estimate")), "")
        serials = {
            rec.fields.get("serial_number", "")
            for rec in recs
            if rec.fields.get("serial_number")
        }
        items: List[str] = []
        for item_col in ("item_1", "item_2", "item_3", "item_4"):
            for rec in recs:
                value = rec.fields.get(item_col, "")
                if value:
                    items.append(value)
        existing_count = sum(1 for rec in recs if rec.source == "existing")
        new_count = sum(1 for rec in recs if rec.source == "database_a")

        customer_rows.append(
            [
                customer_id,
                primary_name,
                first_name,
                format_date(first_date),
                format_date(last_date),
                str(transaction_count),
                primary_phone,
                "; ".join(sorted(filter(None, alt_phones))),
                email,
                billing,
                shipping,
                district,
                legacy_info,
                sold_from,
                handler,
                "; ".join(sorted(filter(None, remarks))),
                referrer,
                failed_trial,
                unreasonable,
                wealth,
                "; ".join(sorted(serials)),
                "; ".join(sorted(dict.fromkeys(items))),
                str(existing_count),
                str(new_count),
            ]
        )

    customers_path = output_dir / "database_b_customer_summary.csv"
    with customers_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(customer_headers)
        writer.writerows(customer_rows)

    audit_path: Optional[Path] = None
    if match_logs:
        audit_path = output_dir / "database_b_match_audit.csv"
        with audit_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "customer_id",
                    "customer_key",
                    "source",
                    "match_type",
                    "transaction_date",
                    "transaction_name",
                    "normalized_phone",
                    "normalized_serial",
                    "matched_customer_key",
                    "matched_name",
                    "matched_phone",
                ]
            )
            for entry in match_logs:
                writer.writerow(
                    [
                        customer_id_map.get(entry.customer_key, ""),
                        entry.customer_key,
                        entry.source,
                        entry.match_type,
                        format_date(entry.record_date),
                        entry.record_name,
                        entry.record_phone,
                        entry.record_serial,
                        entry.matched_key or "",
                        entry.matched_name,
                        entry.matched_phone,
                    ]
                )

    review_path: Optional[Path] = None
    if manual_review:
        review_path = output_dir / "database_b_manual_review.csv"
        with review_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "customer_id",
                    "customer_key",
                    "match_type",
                    "transaction_name",
                    "transaction_phone",
                    "matched_name",
                    "matched_phone",
                    "reason",
                ]
            )
            for entry in manual_review:
                writer.writerow(
                    [
                        customer_id_map.get(entry.customer_key, ""),
                        entry.customer_key,
                        entry.match_type,
                        entry.record_name,
                        entry.record_phone,
                        entry.matched_name,
                        entry.matched_phone,
                        entry.reason,
                    ]
                )

    summary_path = output_dir / "database_b_summary.txt"
    with summary_path.open("w", encoding="utf-8") as f:
        total_customers = len(sorted_keys)
        total_transactions = len(records)
        new_transactions = sum(1 for record in records if record.source == "database_a")
        latest_date = max((rec.date for rec in records if rec.date), default=None)
        manual_review_count = len(manual_review)
        f.write(
            "Database B rebuild summary\n"
            f"Total customers: {total_customers}\n"
            f"Total transactions: {total_transactions}\n"
            f"Transactions added from Database A (>= 2025-08-28): {new_transactions}\n"
            f"Latest transaction date: {format_date(latest_date)}\n"
            f"Potential manual review items: {manual_review_count}\n"
        )

    print(f"Wrote {transactions_path}")
    print(f"Wrote {customers_path}")
    if audit_path:
        print(f"Wrote {audit_path}")
    if review_path:
        print(f"Wrote {review_path}")
    print(f"Wrote {summary_path}")


if __name__ == "__main__":
    main(sys.argv[1:])
