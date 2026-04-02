from __future__ import annotations

import streamlit as st

from dpp_logic import (
    build_dpp,
    build_map_data,
    build_validation_report,
    combine_files,
    dataframe_to_csv_bytes,
    find_final_candidates,
    graph_to_dot,
    json_bytes,
    make_pdf_report,
    trace_upstream,
)

try:
    import pydeck as pdk
except ImportError:  # pragma: no cover
    pdk = None


st.set_page_config(page_title="UNTP DPP PoC Validator", layout="wide")

st.title("UNTP DPP PoC Validator")
st.caption(
    "Carga entre 3 y 5 formularios Excel de eventos, valida la cadena de trazabilidad real, muestra el recorrido geográfico y genera el DPP con reporte PDF."
)


def render_file_summary(df):
    summary = (
        df.groupby("source_file", dropna=False)
        .agg(
            rows=("event_id", "size"),
            stage_orders=("stage_order", lambda values: sorted({int(v) for v in values.dropna().tolist()})),
            event_types=("event_type", lambda values: sorted({v for v in values.tolist() if v})),
            geo_points=("geo_lat", lambda values: int(values.notna().sum())),
        )
        .reset_index()
        .rename(columns={"source_file": "file_name"})
    )
    st.dataframe(summary, use_container_width=True)


def render_validation_report(validation_report):
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


def render_map(trace_df):
    st.subheader("Mapa de eventos")
    map_data = build_map_data(trace_df)

    if map_data["missing_coordinates"]:
        st.info(
            "Eventos sin coordenadas: " + ", ".join(map_data["missing_coordinates"])
        )

    if not map_data["points"]:
        st.warning("No hay coordenadas disponibles para renderizar el mapa.")
        return

    if pdk is None:
        st.warning("`pydeck` no está instalado en el entorno actual, así que el mapa no puede renderizarse.")
        return

    point_layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_data["points"],
        get_position="[lon, lat]",
        get_fill_color="color",
        get_radius=1500,
        pickable=True,
    )
    layers = [point_layer]

    if map_data["segments"]:
        layers.append(
            pdk.Layer(
                "ArcLayer",
                data=map_data["segments"],
                get_source_position="[from_lon, from_lat]",
                get_target_position="[to_lon, to_lat]",
                get_width=4,
                get_source_color="color",
                get_target_color="color",
            )
        )

    view = pdk.ViewState(
        latitude=map_data["center"]["latitude"],
        longitude=map_data["center"]["longitude"],
        zoom=map_data["center"]["zoom"],
    )
    tooltip = {
        "html": "<b>{event_id}</b><br/>Etapa {stage_order}<br/>{facility_name}<br/>{region}, {country}<br/>{date_start}",
        "style": {"backgroundColor": "#0f172a", "color": "white"},
    }
    st.pydeck_chart(pdk.Deck(layers=layers, initial_view_state=view, tooltip=tooltip), use_container_width=True)


def render_event_table(trace_df):
    display_columns = [
        "stage_order",
        "event_id",
        "event_type",
        "actor_name",
        "facility_name",
        "event_date_start",
        "event_date_end",
        "input_lot_ids",
        "output_lot_id",
        "geo_lat",
        "geo_lon",
        "source_file",
    ]
    available_columns = [column for column in display_columns if column in trace_df.columns]
    st.dataframe(trace_df[available_columns], use_container_width=True)


uploaded_files = st.file_uploader(
    "Sube archivos Excel (`.xlsx`)",
    type=["xlsx"],
    accept_multiple_files=True,
)

if not uploaded_files:
    st.info("Sube entre 3 y 5 archivos para analizar una cadena de trazabilidad.")
    st.stop()

if len(uploaded_files) < 3 or len(uploaded_files) > 5:
    st.error("La cantidad de archivos debe estar entre 3 y 5.")
    st.stop()

try:
    combined_df = combine_files(uploaded_files)
except Exception as exc:
    st.exception(exc)
    st.stop()

candidates = find_final_candidates(combined_df)
if not candidates:
    st.error("No se pudo detectar un lote final candidato.")
    st.stop()

sidebar = st.sidebar
sidebar.header("Configuración")
selected_lot = sidebar.selectbox("Lote final candidato", candidates)

trace_result = trace_upstream(selected_lot, combined_df)
validation_report = build_validation_report(combined_df, trace_result)
dpp = build_dpp(selected_lot, combined_df, trace_result, validation_report)
is_valid = dpp["validation"]["status"] == "ready"

st.subheader("Resumen")
summary_columns = st.columns(4)
summary_columns[0].metric("Lote final", selected_lot)
summary_columns[1].metric("Estado DPP", dpp["validation"]["status"])
summary_columns[2].metric("Eventos en cadena", len(trace_result["event_ids"]))
summary_columns[3].metric("Puntos con mapa", len(dpp["locations"]))

tab_overview, tab_trace, tab_dpp = st.tabs(["Validación", "Cadena", "DPP y descargas"])

with tab_overview:
    st.subheader("Archivos cargados")
    render_file_summary(combined_df)
    st.subheader("Reporte de validación")
    render_validation_report(validation_report)
    with st.expander("Tabla consolidada"):
        st.dataframe(combined_df, use_container_width=True)

with tab_trace:
    left_col, right_col = st.columns([1.1, 0.9])
    with left_col:
        st.subheader("Tabla de cadena")
        render_event_table(trace_result["trace_df"])
        st.subheader("Grafo de trazabilidad")
        st.graphviz_chart(graph_to_dot(trace_result["graph"]), use_container_width=True)
    with right_col:
        render_map(trace_result["trace_df"])

with tab_dpp:
    if is_valid:
        st.success("El DPP está listo para descarga.")
    else:
        st.error("El DPP no es descargable como documento válido porque la cadena seleccionada es inválida.")

    st.json(dpp, expanded=2)

    download_columns = st.columns(4)
    if is_valid:
        download_columns[0].download_button(
            "Descargar dpp.json",
            data=json_bytes(dpp),
            file_name="dpp.json",
            mime="application/json",
        )

        try:
            pdf_bytes = make_pdf_report(dpp, trace_result["trace_df"], validation_report)
        except RuntimeError as exc:
            st.warning(str(exc))
        else:
            download_columns[1].download_button(
                "Descargar reporte PDF",
                data=pdf_bytes,
                file_name=f"{selected_lot.lower()}_report.pdf",
                mime="application/pdf",
            )

    download_columns[2].download_button(
        "Descargar traceability_chain.csv",
        data=dataframe_to_csv_bytes(trace_result["trace_df"]),
        file_name="traceability_chain.csv",
        mime="text/csv",
    )
    download_columns[3].download_button(
        "Descargar validation_report.json",
        data=json_bytes(validation_report),
        file_name="validation_report.json",
        mime="application/json",
    )
