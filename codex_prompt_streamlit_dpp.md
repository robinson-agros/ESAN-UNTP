# Prompt para Codex: Streamlit PoC de Digital Product Passport (DPP) basado en eventos Excel

Quiero que generes una aplicación Streamlit llamada `app.py` para una prueba de concepto de DPP alineada con la lógica de UNTP:
- cada etapa del proceso se captura en un formulario;
- cada formulario se exporta como un archivo `.xlsx`;
- el usuario sube entre **3 y 5 archivos Excel**;
- la app debe validar si existe una cadena de trazabilidad consistente entre etapas;
- si las conexiones existen, la app debe consolidar la información y generar un **Digital Product Passport (DPP)**;
- si faltan conexiones o hay lotes huérfanos, la app debe mostrar errores claros y no emitir el DPP.

## 1) Objetivo funcional

Construye una app Streamlit que:
1. Permita subir entre 3 y 5 archivos `.xlsx`.
2. Lea cada Excel (una hoja `form_event`, aunque si no existe debe tomar la primera hoja).
3. Estandarice columnas y combine todos los archivos en un solo DataFrame.
4. Valide la trazabilidad basada en:
   - `input_lot_ids`
   - `output_lot_id`
   - `event_id`
   - `stage_order`
5. Detecte cuál es el lote final candidato a DPP:
   - preferir filas que tengan `dpp_candidate_id`;
   - si hay varias, permitir elegir en un selectbox;
   - si no hay ninguna, inferir el lote final como un `output_lot_id` que no vuelve a aparecer como input.
6. Recorra la cadena aguas arriba desde el lote final hasta los lotes origen.
7. Genere un DPP en JSON y una vista legible en pantalla.
8. Permita descargar:
   - `dpp.json`
   - `traceability_chain.csv`
   - `validation_report.json`

## 2) Reglas de negocio

### 2.1 Cantidad de etapas
- mínimo 3 archivos
- máximo 5 archivos
- mostrar error si el usuario sube menos de 3 o más de 5

### 2.2 Estructura esperada por fila
Cada fila representa un evento y debería usar estas columnas (si faltan algunas no críticas, tolerarlo; si faltan críticas, marcar error):
- `record_id`
- `event_id`
- `event_type`
- `stage_order`
- `actor_id`
- `actor_name`
- `facility_id`
- `facility_name`
- `event_date_start`
- `event_date_end`
- `country`
- `region`
- `product_name`
- `input_lot_ids`
- `output_lot_id`
- `input_quantity_kg`
- `output_quantity_kg`
- `unit`
- `process_notes`
- `evidence_ref`
- `related_doc_ref`
- `dpp_candidate_id`
- `product_id`
- `batch_number`
- `product_category`

### 2.3 Reglas de parseo
- `input_lot_ids` puede venir vacío en la primera etapa.
- si contiene múltiples lotes, vienen separados por `|`.
- normalizar espacios, mayúsculas y valores vacíos.
- `stage_order` debe convertirse a entero.
- `event_date_start` y `event_date_end` deben parsearse como fecha si es posible.

### 2.4 Reglas de validación de cadena
Implementa validaciones claras:

#### Validaciones estructurales
- no puede haber `event_id` duplicados
- no puede haber `output_lot_id` vacío
- no puede haber `output_lot_id` duplicado entre eventos, salvo que explícitamente decidas soportarlo y lo documentes (por defecto: error)
- `stage_order` debe estar entre 1 y 5

#### Validaciones de conectividad
- todo `input_lot_id` no vacío debe existir como `output_lot_id` de algún evento previo
- un evento no puede consumir un lote generado en una etapa posterior
- la cadena del lote final debe ser navegable hacia atrás hasta llegar a uno o más lotes origen
- marcar como huérfanos los lotes generados que no participan en la cadena del DPP seleccionado
- si la cadena del lote final tiene referencias rotas, NO generar DPP

#### Validaciones opcionales pero deseables
- advertir si `input_quantity_kg < output_quantity_kg`
- advertir si faltan `product_id` o `batch_number` en la etapa final
- advertir si hay saltos de etapa (por ejemplo de 1 a 4) aunque permitirlo si la cadena es válida

## 3) Qué debe mostrar la app

## Pantalla principal
- título: `UNTP DPP PoC Validator`
- explicación corta
- uploader múltiple de Excel
- botón `Procesar archivos`

## Al procesar
Mostrar estas secciones:

### A. Resumen de archivos
- nombre del archivo
- cantidad de filas
- stage_order detectados
- event_type presentes

### B. Tabla consolidada
- DataFrame combinado y ordenado por `stage_order`, `event_date_start`, `event_id`

### C. Reporte de validación
- errores
- warnings
- métricas:
  - número de eventos
  - número de lotes
  - número de lotes finales candidatos
  - número de lotes huérfanos
  - número de referencias rotas

### D. Selección del lote final / DPP
- si hay un único candidato, seleccionarlo automáticamente
- si hay varios, usar `st.selectbox`

### E. Visualización de cadena
- mostrar una tabla de la cadena upstream
- mostrar además un grafo simple con `networkx` + `pyvis` o con `graphviz`:
  - nodos de tipo lote
  - nodos de tipo evento
  - relación input lot -> event -> output lot

### F. DPP generado
Mostrar un JSON con esta estructura mínima:

```json
{
  "dpp_id": "DPP-LOT-EXP-001",
  "product_id": "PROD-COCOA-BEAN-001",
  "id_granularity": "batch",
  "final_lot_id": "LOT-EXP-001",
  "batch_number": "BATCH-EXP-2026-001",
  "product_name": "Grano de cacao seco exportable",
  "product_category": "finished_batch",
  "issuer": {
    "actor_id": "ORG-EXP-01",
    "actor_name": "Exportadora Andina",
    "facility_id": "FAC-EXP-01",
    "facility_name": "Puerto de Salida"
  },
  "timeline": {
    "start_date": "2026-02-01",
    "end_date": "2026-02-18",
    "stage_count": 5
  },
  "traceability": {
    "event_ids": [],
    "lot_ids": [],
    "root_lot_ids": []
  },
  "supporting_documents": [],
  "validation": {
    "status": "ready",
    "errors": [],
    "warnings": []
  }
}
```

## 4) Algoritmo esperado

Implementa funciones limpias y testeables:

- `load_excel(file) -> pd.DataFrame`
- `normalize_dataframe(df) -> pd.DataFrame`
- `combine_files(files) -> pd.DataFrame`
- `validate_structure(df) -> dict`
- `build_lineage_index(df) -> dict`
- `find_final_candidates(df) -> list[str]`
- `trace_upstream(final_lot_id, df) -> dict`
- `build_dpp(final_lot_id, df, trace_result, validation_report) -> dict`

## 4.1 Índices recomendados
Construye como mínimo:
- `event_by_id`
- `event_by_output_lot`
- `events_by_stage`
- `inputs_by_event`

## 4.2 Trazabilidad
Para el lote final:
1. encontrar el evento que produce ese lote
2. obtener sus `input_lot_ids`
3. por cada input lot, buscar el evento que lo produjo
4. repetir recursivamente o con DFS/BFS
5. detenerse cuando un lote no tenga inputs (lote origen)

## 4.3 Detección de errores
Durante el DFS/BFS:
- si un input lot no existe como output en ningún evento previo, registrar referencia rota
- si detectas ciclos, registrar error de ciclo
- si la cadena tiene errores, el estado final del DPP debe ser `invalid` y la app no debe ofrecer descarga de `dpp.json`

## 5) Requisitos técnicos
Usa:
- Python 3.11+
- streamlit
- pandas
- openpyxl
- networkx
- graphviz o pyvis
- pydantic opcional para validar el JSON final

Genera también:
- `requirements.txt`
- `README.md`

## 6) UX y robustez
- Manejo claro de errores
- No asumir nombres exactos de archivo
- Detectar `stage_order` desde el contenido, no desde el nombre del archivo
- Si un archivo contiene más de una fila, soportarlo
- Si una columna no existe, crearla vacía si no es crítica
- Botones de descarga al final

## 7) Tests mínimos
Incluye funciones o tests simples para probar:
- dataset válido de 3 etapas
- dataset válido de 5 etapas
- dataset inválido con referencia rota

## 8) Entregables esperados
Quiero que generes:
- `app.py`
- `requirements.txt`
- `README.md`
- opcionalmente `utils.py` o `dpp_logic.py` si ayuda a mantener el código limpio

## 9) Criterio de aceptación
La app se considera correcta si:
- con el dataset válido de 3 etapas genera DPP
- con el dataset válido de 5 etapas genera DPP
- con el dataset inválido detecta la referencia rota y no genera DPP
- muestra claramente la cadena de trazabilidad
- permite descargar los resultados

## 10) Importante
- No simplifiques la lógica a “solo unir Excel por orden”.
- La validación debe depender de la relación real entre `input_lot_ids` y `output_lot_id`.
- El DPP debe construirse solo si la cadena seleccionada es consistente.
- El código debe ser claro, modular y listo para evolucionar después a credenciales verificables / JSON-LD.