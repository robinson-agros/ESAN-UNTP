# UNTP DPP PoC Validator

Aplicación Streamlit para validar una cadena de trazabilidad basada en eventos Excel y emitir un Digital Product Passport (DPP) solo cuando la cadena seleccionada es consistente. La app también muestra el recorrido geográfico de la cadena cuando los eventos incluyen coordenadas y puede exportar un reporte PDF.

## Archivos principales

- `app.py`: interfaz Streamlit.
- `dpp_logic.py`: carga, normalización, validación, trazabilidad y construcción del DPP.
- `tests/test_dpp_logic.py`: pruebas mínimas sobre los datasets provistos.

## Requisitos

- Python 3.11+
- Dependencias de `requirements.txt`

## Instalación

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Ejecutar la app

```bash
streamlit run app.py
```

## Uso

1. Sube entre 3 y 5 archivos `.xlsx`.
2. La app detecta la hoja `form_event` o usa la primera hoja disponible.
3. Se consolida la información, se validan reglas estructurales y de conectividad, y se detectan candidatos a lote final.
4. La app permite seleccionar el lote final candidato desde la barra lateral y mantiene la visualización al cambiar de lote.
5. Si los eventos incluyen `geo_lat` y `geo_lon`, se muestra un mapa con el recorrido entre etapas.
6. Si la cadena del lote seleccionado es válida, la app genera el DPP y habilita la descarga de `dpp.json` y un reporte PDF.
7. Siempre permite descargar `traceability_chain.csv` y `validation_report.json`.

## Validaciones implementadas

- `event_id` duplicado.
- `output_lot_id` vacío o duplicado.
- `stage_order` inválido o fuera de `1..5`.
- referencias rotas entre `input_lot_ids` y `output_lot_id`.
- consumo de un lote producido en etapa posterior.
- ciclos en la cadena upstream.
- advertencias por crecimiento de cantidad, falta de `product_id` o `batch_number` final, y saltos de etapa.
- detección de lotes huérfanos fuera de la cadena del DPP seleccionado.
- resumen enriquecido del DPP con eventos y ubicaciones por etapa.

## Ejecutar tests

```bash
pytest
```

## Datasets de ejemplo

- `dataset_valid_3_stage/`
- `dataset_valid_5_stage/`
- `dataset_invalid_4_stage_missing_link/`

Los archivos `expected_dpp_valid_3_stage.json` y `expected_dpp_valid_5_stage.json` sirven como referencia para la salida esperada en los escenarios válidos.

Los datasets de ejemplo ya incluyen columnas `geo_lat` y `geo_lon` para demostrar la funcionalidad de mapa.
