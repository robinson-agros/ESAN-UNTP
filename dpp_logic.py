from __future__ import annotations

import io
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import networkx as nx
import pandas as pd


EXPECTED_COLUMNS = [
    "record_id",
    "event_id",
    "event_type",
    "stage_order",
    "actor_id",
    "actor_name",
    "facility_id",
    "facility_name",
    "event_date_start",
    "event_date_end",
    "country",
    "region",
    "geo_lat",
    "geo_lon",
    "product_name",
    "input_lot_ids",
    "output_lot_id",
    "input_quantity_kg",
    "output_quantity_kg",
    "unit",
    "process_notes",
    "evidence_ref",
    "related_doc_ref",
    "dpp_candidate_id",
    "product_id",
    "batch_number",
    "product_category",
]

CRITICAL_COLUMNS = ["event_id", "stage_order", "output_lot_id"]
ID_COLUMNS = [
    "record_id",
    "event_id",
    "actor_id",
    "facility_id",
    "input_lot_ids",
    "output_lot_id",
    "dpp_candidate_id",
    "product_id",
    "batch_number",
]
TEXT_COLUMNS = [
    "event_type",
    "actor_name",
    "facility_name",
    "country",
    "region",
    "product_name",
    "unit",
    "process_notes",
    "evidence_ref",
    "related_doc_ref",
    "product_category",
]
DATE_COLUMNS = ["event_date_start", "event_date_end"]
NUMERIC_COLUMNS = ["input_quantity_kg", "output_quantity_kg", "geo_lat", "geo_lon"]


@dataclass
class WorkbookLoadResult:
    dataframe: pd.DataFrame
    sheet_name: str
    header_row: int


def normalize_column_name(value: Any) -> str:
    return str(value).strip().lower().replace(" ", "_")


def clean_scalar(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, str):
        cleaned = " ".join(value.strip().split())
        return cleaned or None
    return value


def normalize_identifier(value: Any) -> str:
    cleaned = clean_scalar(value)
    if cleaned is None:
        return ""
    return str(cleaned).upper()


def parse_input_lots(value: Any) -> list[str]:
    cleaned = clean_scalar(value)
    if cleaned is None:
        return []
    parts = [normalize_identifier(part) for part in str(cleaned).split("|")]
    return [part for part in parts if part]


def detect_header_row(raw_df: pd.DataFrame) -> int:
    normalized_rows = raw_df.fillna("").astype(str).map(normalize_column_name)
    for idx, row in normalized_rows.iterrows():
        row_values = set(row.tolist())
        if {"event_id", "stage_order", "output_lot_id"}.issubset(row_values):
            return int(idx)
    raise ValueError("No se pudo detectar una fila de encabezado con columnas críticas.")


def load_excel(file: Any) -> pd.DataFrame:
    workbook = pd.ExcelFile(file)
    sheet_name = "form_event" if "form_event" in workbook.sheet_names else workbook.sheet_names[0]
    raw_df = pd.read_excel(workbook, sheet_name=sheet_name, header=None, dtype=object)
    header_row = detect_header_row(raw_df)
    header_values = [normalize_column_name(value) for value in raw_df.iloc[header_row].tolist()]
    data = raw_df.iloc[header_row + 1 :].copy()
    data.columns = header_values
    data = data.reset_index(drop=True)
    data = data.dropna(how="all")
    return data


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    normalized.columns = [normalize_column_name(column) for column in normalized.columns]

    for column in EXPECTED_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = None

    normalized = normalized[EXPECTED_COLUMNS + [col for col in normalized.columns if col not in EXPECTED_COLUMNS]]

    for column in ID_COLUMNS:
        normalized[column] = normalized[column].map(normalize_identifier)

    for column in TEXT_COLUMNS:
        normalized[column] = normalized[column].map(clean_scalar)

    for column in DATE_COLUMNS:
        normalized[column] = pd.to_datetime(normalized[column], errors="coerce")

    for column in NUMERIC_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized["input_lot_ids_list"] = normalized["input_lot_ids"].map(parse_input_lots)
    normalized["stage_order"] = pd.to_numeric(normalized["stage_order"], errors="coerce").astype("Int64")
    normalized["event_type"] = normalized["event_type"].fillna("").str.lower()
    normalized["source_file"] = normalized.get("source_file", "").fillna("")

    sort_columns = ["stage_order", "event_date_start", "event_id"]
    normalized = normalized.sort_values(sort_columns, kind="stable", na_position="last").reset_index(drop=True)
    return normalized


def combine_files(files: list[Any]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for file in files:
        dataframe = load_excel(file)
        dataframe["source_file"] = getattr(file, "name", "uploaded.xlsx")
        frames.append(normalize_dataframe(dataframe))
    if not frames:
        return pd.DataFrame(columns=EXPECTED_COLUMNS + ["input_lot_ids_list", "source_file"])
    return pd.concat(frames, ignore_index=True)


def format_date(value: Any) -> str:
    if pd.isna(value) or value is None:
        return ""
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    return str(value)


def validate_structure(df: pd.DataFrame) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []

    missing_critical = [column for column in CRITICAL_COLUMNS if column not in df.columns]
    if missing_critical:
        errors.append(f"Faltan columnas críticas: {', '.join(missing_critical)}")
        return {
            "errors": errors,
            "warnings": warnings,
            "broken_references": [],
            "orphan_lots": [],
            "metrics": {},
        }

    duplicate_events = df.loc[df["event_id"].eq("") | df["event_id"].isna(), "event_id"]
    if not duplicate_events.empty:
        errors.append("Hay eventos con event_id vacío.")

    repeated_event_ids = df["event_id"][df["event_id"].ne("") & df["event_id"].duplicated()].unique().tolist()
    if repeated_event_ids:
        errors.append(f"Hay event_id duplicados: {', '.join(repeated_event_ids)}")

    empty_output_rows = df.index[df["output_lot_id"].eq("") | df["output_lot_id"].isna()].tolist()
    if empty_output_rows:
        errors.append(f"Hay {len(empty_output_rows)} eventos con output_lot_id vacío.")

    repeated_output_lots = (
        df["output_lot_id"][df["output_lot_id"].ne("") & df["output_lot_id"].duplicated()].unique().tolist()
    )
    if repeated_output_lots:
        errors.append(f"Hay output_lot_id duplicados: {', '.join(repeated_output_lots)}")

    invalid_stage = df.loc[df["stage_order"].isna() | ~df["stage_order"].between(1, 5), "event_id"].tolist()
    if invalid_stage:
        errors.append(f"Hay eventos con stage_order fuera de rango o inválido: {', '.join(invalid_stage)}")

    event_by_output = (
        df.loc[df["output_lot_id"].ne(""), ["output_lot_id", "event_id", "stage_order"]]
        .set_index("output_lot_id")
        .to_dict("index")
    )

    broken_references: list[dict[str, Any]] = []
    for row in df.itertuples(index=False):
        for input_lot in row.input_lot_ids_list:
            producer = event_by_output.get(input_lot)
            if producer is None:
                broken_references.append(
                    {
                        "event_id": row.event_id,
                        "input_lot_id": input_lot,
                        "reason": "missing_output_reference",
                    }
                )
                continue
            if pd.notna(row.stage_order) and producer["stage_order"] > int(row.stage_order):
                errors.append(
                    f"El evento {row.event_id} consume el lote {input_lot} desde una etapa posterior."
                )

    if broken_references:
        broken_list = ", ".join(
            f"{item['input_lot_id']} -> {item['event_id']}" for item in broken_references
        )
        errors.append(f"Hay referencias rotas en la cadena: {broken_list}")

    quantity_warnings = df.loc[
        df["input_quantity_kg"].notna()
        & df["output_quantity_kg"].notna()
        & (df["input_quantity_kg"] < df["output_quantity_kg"]),
        ["event_id", "input_quantity_kg", "output_quantity_kg"],
    ]
    for row in quantity_warnings.itertuples(index=False):
        warnings.append(
            f"El evento {row.event_id} tiene input_quantity_kg ({row.input_quantity_kg}) menor que output_quantity_kg ({row.output_quantity_kg})."
        )

    stages = sorted(stage for stage in df["stage_order"].dropna().astype(int).unique().tolist())
    if len(stages) > 1:
        expected = list(range(stages[0], stages[-1] + 1))
        if stages != expected:
            warnings.append(f"Hay saltos de etapa detectados: {stages}")

    metrics = {
        "event_count": int(df["event_id"].nunique()),
        "lot_count": int(df["output_lot_id"][df["output_lot_id"].ne("")].nunique()),
        "final_candidate_count": len(find_final_candidates(df)),
        "orphan_lot_count": 0,
        "broken_reference_count": len(broken_references),
    }

    return {
        "errors": errors,
        "warnings": warnings,
        "broken_references": broken_references,
        "orphan_lots": [],
        "metrics": metrics,
    }


def build_lineage_index(df: pd.DataFrame) -> dict[str, Any]:
    event_records = df.set_index("event_id").to_dict("index")
    event_by_output_lot = (
        df.loc[df["output_lot_id"].ne("")].set_index("output_lot_id").to_dict("index")
    )
    events_by_stage = {
        int(stage): stage_df["event_id"].tolist()
        for stage, stage_df in df.groupby("stage_order", dropna=True)
    }
    inputs_by_event = {
        row.event_id: list(row.input_lot_ids_list)
        for row in df[["event_id", "input_lot_ids_list"]].itertuples(index=False)
    }
    return {
        "event_by_id": event_records,
        "event_by_output_lot": event_by_output_lot,
        "events_by_stage": events_by_stage,
        "inputs_by_event": inputs_by_event,
    }


def find_final_candidates(df: pd.DataFrame) -> list[str]:
    explicit = [lot for lot in df.loc[df["dpp_candidate_id"].ne(""), "output_lot_id"].tolist() if lot]
    if explicit:
        return list(dict.fromkeys(explicit))

    input_lots = {lot for lots in df["input_lot_ids_list"] for lot in lots}
    outputs = [lot for lot in df["output_lot_id"].tolist() if lot]
    inferred = [lot for lot in outputs if lot not in input_lots]
    return list(dict.fromkeys(inferred))


def trace_upstream(final_lot_id: str, df: pd.DataFrame) -> dict[str, Any]:
    index = build_lineage_index(df)
    event_by_output_lot = index["event_by_output_lot"]
    graph = nx.DiGraph()
    trace_events: list[str] = []
    trace_lots: list[str] = []
    root_lots: list[str] = []
    broken_references: list[dict[str, Any]] = []
    cycle_errors: list[str] = []

    visited_events: set[str] = set()
    visiting_events: set[str] = set()
    visited_lots: set[str] = set()

    def visit_lot(lot_id: str) -> None:
        if not lot_id:
            return
        graph.add_node(lot_id, node_type="lot")
        if lot_id not in visited_lots:
            trace_lots.append(lot_id)
            visited_lots.add(lot_id)

        producer = event_by_output_lot.get(lot_id)
        if producer is None:
            root_lots.append(lot_id)
            return

        event_id = producer["event_id"]
        if event_id in visiting_events:
            cycle_errors.append(f"Ciclo detectado alrededor del evento {event_id}.")
            return
        if event_id in visited_events:
            return

        visiting_events.add(event_id)
        visited_events.add(event_id)
        graph.add_node(event_id, node_type="event")
        graph.add_edge(event_id, lot_id)
        trace_events.append(event_id)

        inputs = producer.get("input_lot_ids_list", []) or []
        if not inputs:
            root_lots.append(lot_id)
        for input_lot in inputs:
            graph.add_node(input_lot, node_type="lot")
            graph.add_edge(input_lot, event_id)
            input_producer = event_by_output_lot.get(input_lot)
            if input_producer is None:
                broken_references.append(
                    {
                        "event_id": event_id,
                        "input_lot_id": input_lot,
                        "reason": "missing_output_reference",
                    }
                )
                continue
            if int(input_producer["stage_order"]) > int(producer["stage_order"]):
                broken_references.append(
                    {
                        "event_id": event_id,
                        "input_lot_id": input_lot,
                        "reason": "produced_in_later_stage",
                    }
                )
                continue
            visit_lot(input_lot)

        visiting_events.remove(event_id)

    visit_lot(final_lot_id)

    trace_df = df[df["event_id"].isin(trace_events)].copy()
    trace_df = trace_df.sort_values(["stage_order", "event_date_start", "event_id"], kind="stable")

    return {
        "final_lot_id": final_lot_id,
        "event_ids": trace_events,
        "lot_ids": trace_lots,
        "root_lot_ids": list(dict.fromkeys(root_lots)),
        "broken_references": broken_references,
        "cycle_errors": list(dict.fromkeys(cycle_errors)),
        "graph": graph,
        "trace_df": trace_df,
    }


def detect_orphan_lots(df: pd.DataFrame, trace_result: dict[str, Any]) -> list[str]:
    trace_lots = set(trace_result["lot_ids"])
    output_lots = {lot for lot in df["output_lot_id"].tolist() if lot}
    return sorted(output_lots - trace_lots)


def build_dpp(
    final_lot_id: str,
    df: pd.DataFrame,
    trace_result: dict[str, Any],
    validation_report: dict[str, Any],
) -> dict[str, Any]:
    final_event = df.loc[df["output_lot_id"] == final_lot_id].sort_values("stage_order").tail(1)
    if final_event.empty:
        raise ValueError(f"No existe un evento productor para el lote final {final_lot_id}.")

    final_row = final_event.iloc[0]
    trace_df = trace_result["trace_df"]
    ready = not validation_report["errors"] and not trace_result["broken_references"] and not trace_result["cycle_errors"]
    status = "ready" if ready else "invalid"
    ordered_event_ids = trace_df["event_id"].tolist()
    ordered_lot_ids = [lot for lot in trace_df["output_lot_id"].tolist() if lot]

    warnings = list(validation_report["warnings"])
    if not final_row.get("product_id"):
        warnings.append("Falta product_id en la etapa final.")
    if not final_row.get("batch_number"):
        warnings.append("Falta batch_number en la etapa final.")

    supporting_documents = sorted(
        {
            value
            for value in trace_df["evidence_ref"].dropna().astype(str).tolist()
            + trace_df["related_doc_ref"].dropna().astype(str).tolist()
            if value
        }
    )

    start_date = trace_df["event_date_start"].min()
    end_date = trace_df["event_date_end"].max()

    return {
        "dpp_id": final_row["dpp_candidate_id"] or f"DPP-{final_lot_id}",
        "product_id": final_row["product_id"] or "",
        "id_granularity": "batch",
        "final_lot_id": final_lot_id,
        "batch_number": final_row["batch_number"] or "",
        "product_name": final_row["product_name"] or "",
        "product_category": final_row["product_category"] or "",
        "issuer": {
            "actor_id": final_row["actor_id"] or "",
            "actor_name": final_row["actor_name"] or "",
            "facility_id": final_row["facility_id"] or "",
            "facility_name": final_row["facility_name"] or "",
        },
        "timeline": {
            "start_date": start_date.date().isoformat() if pd.notna(start_date) else None,
            "end_date": end_date.date().isoformat() if pd.notna(end_date) else None,
            "stage_count": int(trace_df["stage_order"].nunique()),
        },
        "event_summary": build_event_summary(trace_df),
        "locations": build_location_summary(trace_df),
        "traceability": {
            "event_ids": ordered_event_ids,
            "lot_ids": ordered_lot_ids,
            "root_lot_ids": trace_result["root_lot_ids"],
        },
        "supporting_documents": supporting_documents,
        "validation": {
            "status": status,
            "errors": validation_report["errors"] + trace_result["cycle_errors"],
            "warnings": warnings,
        },
    }


def build_validation_report(
    df: pd.DataFrame,
    trace_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    report = validate_structure(df)
    if trace_result is not None:
        orphan_lots = detect_orphan_lots(df, trace_result)
        report["orphan_lots"] = orphan_lots
        report["metrics"]["orphan_lot_count"] = len(orphan_lots)
        if trace_result["broken_references"]:
            report["errors"].append("La cadena del lote final seleccionado tiene referencias rotas.")
        if trace_result["cycle_errors"]:
            report["errors"].extend(trace_result["cycle_errors"])
    return report


def build_event_summary(trace_df: pd.DataFrame) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    ordered = trace_df.sort_values(["stage_order", "event_date_start", "event_id"], kind="stable")
    for row in ordered.itertuples(index=False):
        summary.append(
            {
                "event_id": row.event_id,
                "stage_order": int(row.stage_order) if pd.notna(row.stage_order) else None,
                "event_type": row.event_type or "",
                "date_start": format_date(row.event_date_start),
                "date_end": format_date(row.event_date_end),
                "actor_name": row.actor_name or "",
                "facility_name": row.facility_name or "",
                "output_lot_id": row.output_lot_id or "",
                "input_lot_ids": list(row.input_lot_ids_list or []),
            }
        )
    return summary


def build_location_summary(trace_df: pd.DataFrame) -> list[dict[str, Any]]:
    locations: list[dict[str, Any]] = []
    for row in trace_df.sort_values(["stage_order", "event_date_start", "event_id"], kind="stable").itertuples(index=False):
        if pd.isna(row.geo_lat) or pd.isna(row.geo_lon):
            continue
        locations.append(
            {
                "event_id": row.event_id,
                "stage_order": int(row.stage_order) if pd.notna(row.stage_order) else None,
                "facility_id": row.facility_id or "",
                "facility_name": row.facility_name or "",
                "country": row.country or "",
                "region": row.region or "",
                "geo_lat": float(row.geo_lat),
                "geo_lon": float(row.geo_lon),
            }
        )
    return locations


def build_map_data(trace_df: pd.DataFrame) -> dict[str, Any]:
    ordered = trace_df.sort_values(["stage_order", "event_date_start", "event_id"], kind="stable")
    points: list[dict[str, Any]] = []
    missing_coordinates: list[str] = []
    palette = [
        [15, 118, 110],
        [217, 119, 6],
        [37, 99, 235],
        [220, 38, 38],
        [124, 58, 237],
        [20, 184, 166],
    ]

    for idx, row in enumerate(ordered.itertuples(index=False)):
        if pd.isna(row.geo_lat) or pd.isna(row.geo_lon):
            missing_coordinates.append(row.event_id)
            continue
        points.append(
            {
                "event_id": row.event_id,
                "stage_order": int(row.stage_order) if pd.notna(row.stage_order) else None,
                "event_type": row.event_type or "",
                "facility_name": row.facility_name or "",
                "region": row.region or "",
                "country": row.country or "",
                "date_start": format_date(row.event_date_start),
                "lat": float(row.geo_lat),
                "lon": float(row.geo_lon),
                "color": palette[idx % len(palette)],
                "tooltip": " | ".join(
                    part
                    for part in [
                        row.event_id or "",
                        f"Etapa {int(row.stage_order)}" if pd.notna(row.stage_order) else "",
                        row.facility_name or "",
                    ]
                    if part
                ),
            }
        )

    segments = []
    for idx in range(len(points) - 1):
        segments.append(
            {
                "from_lon": points[idx]["lon"],
                "from_lat": points[idx]["lat"],
                "to_lon": points[idx + 1]["lon"],
                "to_lat": points[idx + 1]["lat"],
                "color": points[idx]["color"],
            }
        )

    center = None
    if points:
        center = {
            "latitude": sum(point["lat"] for point in points) / len(points),
            "longitude": sum(point["lon"] for point in points) / len(points),
            "zoom": 5 if len(points) > 1 else 7,
        }

    return {
        "points": points,
        "segments": segments,
        "center": center,
        "missing_coordinates": missing_coordinates,
    }


def make_pdf_report(
    dpp: dict[str, Any],
    trace_df: pd.DataFrame,
    validation_report: dict[str, Any],
) -> bytes:
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import cm
        from reportlab.pdfgen import canvas
    except ImportError as exc:
        raise RuntimeError("La dependencia 'reportlab' no está instalada.") from exc

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    margin = 2 * cm
    y = height - margin

    def ensure_space(lines: int = 1) -> None:
        nonlocal y
        if y < margin + (lines * 0.6 * cm):
            pdf.showPage()
            y = height - margin

    def write_line(text: str, *, font: str = "Helvetica", size: int = 10, gap: float = 0.5) -> None:
        nonlocal y
        ensure_space()
        pdf.setFont(font, size)
        pdf.drawString(margin, y, str(text)[:140])
        y -= gap * cm

    pdf.setTitle(f"DPP {dpp['dpp_id']}")
    write_line("Digital Product Passport", font="Helvetica-Bold", size=16, gap=0.8)
    write_line(f"DPP ID: {dpp['dpp_id']}", font="Helvetica-Bold", size=11)
    write_line(f"Producto: {dpp['product_name'] or '-'}")
    write_line(f"Lote final: {dpp['final_lot_id']}")
    write_line(f"Batch: {dpp['batch_number'] or '-'}")
    write_line(f"Estado validacion: {dpp['validation']['status']}")
    write_line(f"Emisor: {dpp['issuer']['actor_name'] or '-'}")
    write_line(f"Instalacion final: {dpp['issuer']['facility_name'] or '-'}", gap=0.8)

    write_line("Resumen de eventos", font="Helvetica-Bold", size=12, gap=0.7)
    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(margin, y, "Etapa")
    pdf.drawString(margin + 2.2 * cm, y, "Evento")
    pdf.drawString(margin + 7.0 * cm, y, "Fecha")
    pdf.drawString(margin + 10.2 * cm, y, "Instalacion")
    y -= 0.35 * cm
    pdf.line(margin, y, width - margin, y)
    y -= 0.3 * cm

    for event in build_event_summary(trace_df):
        ensure_space(2)
        pdf.setFont("Helvetica", 8)
        pdf.drawString(margin, y, str(event["stage_order"] or "-"))
        pdf.drawString(margin + 2.2 * cm, y, str(event["event_id"])[:24])
        pdf.drawString(margin + 7.0 * cm, y, str(event["date_start"])[:16])
        pdf.drawString(margin + 10.2 * cm, y, str(event["facility_name"])[:34])
        y -= 0.42 * cm

    y -= 0.2 * cm
    write_line("Observaciones", font="Helvetica-Bold", size=12, gap=0.7)
    issues = validation_report["errors"] or validation_report["warnings"] or ["Sin observaciones relevantes."]
    for issue in issues[:12]:
        write_line(f"- {issue}", size=9, gap=0.45)

    map_data = build_map_data(trace_df)
    if map_data["points"]:
        y -= 0.2 * cm
        write_line("Ubicaciones", font="Helvetica-Bold", size=12, gap=0.7)
        for point in map_data["points"][:12]:
            write_line(
                f"- Etapa {point['stage_order']}: {point['facility_name']} ({point['lat']:.4f}, {point['lon']:.4f})",
                size=9,
                gap=0.45,
            )

    pdf.save()
    buffer.seek(0)
    return buffer.getvalue()


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    export_df = df.copy()
    if "input_lot_ids_list" in export_df.columns:
        export_df["input_lot_ids_list"] = export_df["input_lot_ids_list"].map(lambda lots: "|".join(lots))
    return export_df.to_csv(index=False).encode("utf-8")


def json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")


def graph_to_dot(graph: nx.DiGraph) -> str:
    lines = ["digraph Traceability {"]
    lines.append('  rankdir="LR";')
    for node, attributes in graph.nodes(data=True):
        if attributes.get("node_type") == "event":
            lines.append(f'  "{node}" [shape=box, style=filled, fillcolor="#EAD7A4"];')
        else:
            lines.append(f'  "{node}" [shape=ellipse, style=filled, fillcolor="#D7E8BA"];')
    for source, target in graph.edges():
        lines.append(f'  "{source}" -> "{target}";')
    lines.append("}")
    return "\n".join(lines)


def run_scenario(paths: list[Path | str]) -> dict[str, Any]:
    buffers = []
    for path in paths:
        path_obj = Path(path)
        file_buffer = io.BytesIO(path_obj.read_bytes())
        file_buffer.name = path_obj.name
        buffers.append(file_buffer)

    df = combine_files(buffers)
    candidates = find_final_candidates(df)
    if not candidates:
        raise AssertionError("No se detectaron lotes finales candidatos.")
    trace_result = trace_upstream(candidates[0], df)
    report = build_validation_report(df, trace_result)
    dpp = build_dpp(candidates[0], df, trace_result, report)
    return {
        "df": df,
        "candidates": candidates,
        "trace_result": trace_result,
        "report": report,
        "dpp": dpp,
    }
