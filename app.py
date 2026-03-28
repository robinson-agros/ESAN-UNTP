from __future__ import annotations

import streamlit as st

from dpp_logic import (
    build_dpp,
    build_validation_report,
    combine_files,
    dataframe_to_csv_bytes,
    find_final_candidates,
    graph_to_dot,
    json_bytes,
    trace_upstream,
)


st.set_page_config(page_title="UNTP DPP PoC Validator", layout="wide")

st.title("UNTP DPP PoC Validator")
st.caption(
    "Carga entre 3 y 5 formularios Excel de eventos, valida la cadena de trazabilidad real y genera el DPP solo si la cadena seleccionada es consistente."
)


def render_file_summary(df):
    summary = (
        df.groupby("source_file", dropna=False)
        .agg(
            rows=("event_id", "size"),
            stage_orders=("stage_order", lambda values: sorted({int(v) for v in values.dropna().tolist()})),
            event_types=("event_type", lambda values: sorted({v for v in values.tolist() if v})),
        )
        .reset_index()
        .rename(columns={"source_file": "file_name"})
    )
    st.dataframe(summary, use_container_width=True)


uploaded_files = st.file_uploader(
    "Sube archivos Excel (`.xlsx`)",
    type=["xlsx"],
    accept_multiple_files=True,
)

if st.button("Procesar archivos", type="primary"):
    if not uploaded_files:
        st.error("Debes subir entre 3 y 5 archivos Excel.")
        st.stop()
    if len(uploaded_files) < 3 or len(uploaded_files) > 5:
        st.error("La cantidad de archivos debe estar entre 3 y 5.")
        st.stop()

    try:
        combined_df = combine_files(uploaded_files)
    except Exception as exc:
        st.exception(exc)
        st.stop()

    st.subheader("A. Resumen de archivos")
    render_file_summary(combined_df)

    st.subheader("B. Tabla consolidada")
    st.dataframe(combined_df, use_container_width=True)

    candidates = find_final_candidates(combined_df)
    if not candidates:
        st.error("No se pudo detectar un lote final candidato.")
        st.stop()

    st.subheader("D. Selección del lote final / DPP")
    if len(candidates) == 1:
        selected_lot = candidates[0]
        st.info(f"Lote final seleccionado automáticamente: `{selected_lot}`")
    else:
        selected_lot = st.selectbox("Selecciona el lote final candidato", candidates)

    trace_result = trace_upstream(selected_lot, combined_df)
    validation_report = build_validation_report(combined_df, trace_result)

    st.subheader("C. Reporte de validación")
    metrics = validation_report["metrics"]
    metric_columns = st.columns(5)
    metric_columns[0].metric("Eventos", metrics["event_count"])
    metric_columns[1].metric("Lotes", metrics["lot_count"])
    metric_columns[2].metric("Candidatos DPP", metrics["final_candidate_count"])
    metric_columns[3].metric("Lotes huérfanos", metrics["orphan_lot_count"])
    metric_columns[4].metric("Referencias rotas", metrics["broken_reference_count"])

    if validation_report["errors"]:
        for error in validation_report["errors"]:
            st.error(error)
    else:
        st.success("No se detectaron errores estructurales ni de conectividad para el lote seleccionado.")

    for warning in validation_report["warnings"]:
        st.warning(warning)

    if validation_report["orphan_lots"]:
        st.warning(f"Lotes huérfanos: {', '.join(validation_report['orphan_lots'])}")

    st.subheader("E. Visualización de cadena")
    st.dataframe(trace_result["trace_df"], use_container_width=True)
    st.graphviz_chart(graph_to_dot(trace_result["graph"]), use_container_width=True)

    dpp = build_dpp(selected_lot, combined_df, trace_result, validation_report)

    st.subheader("F. DPP generado")
    st.json(dpp, expanded=2)

    is_valid = dpp["validation"]["status"] == "ready"
    if is_valid:
        st.download_button(
            "Descargar dpp.json",
            data=json_bytes(dpp),
            file_name="dpp.json",
            mime="application/json",
        )
    else:
        st.error("El DPP no es descargable porque la cadena seleccionada es inválida.")

    st.download_button(
        "Descargar traceability_chain.csv",
        data=dataframe_to_csv_bytes(trace_result["trace_df"]),
        file_name="traceability_chain.csv",
        mime="text/csv",
    )
    st.download_button(
        "Descargar validation_report.json",
        data=json_bytes(validation_report),
        file_name="validation_report.json",
        mime="application/json",
    )
