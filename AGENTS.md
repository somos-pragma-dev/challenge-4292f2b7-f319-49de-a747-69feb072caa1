# AGENTS.md

Instrucciones para el agente de IA que abra este repositorio (Claude Code, Cursor, Codex, Copilot, Gemini). Se cargan solas: no hay que pegar nada en ningun chat.

## Que es este repositorio

Es el codigo base de un reto de aprendizaje de Pragma: **Consolidación de transacciones diarias en el modelo analítico**.

| | |
|---|---|
| Tema | pipelines de transformacion y carga |
| Nivel | advanced-l2 |
| Chapter | Ciencia de Datos — Ingeniero de Datos |
| Especialidad | Ingeniero de datos |
| Stack | Python 3.13 / PySpark 3.5 |
| Patron arquitectonico | ETL con etapas separadas y orquestación en Airflow |
| Tiempo estimado | 10 horas |

## Tu tarea

Dejar este proyecto en estado **verificable**: que el comando de verificacion corra sin errores. Escribi los archivos en disco, en este repositorio. No generes ZIPs ni archivos adjuntos.

En orden:

1. Corre `pip install -r requirements.txt && pytest -q` y mira que falla.
2. Completa lo que falte de la lista de abajo: manifiesto de dependencias, punto de entrada, capa de interfaz y las capas del patron declarado.
3. Arregla SOLO los errores que impiden compilar o arrancar.
4. Volve a correr `pip install -r requirements.txt && pytest -q` hasta que pase.
5. Pará ahí.

## Regla dura: las fases son trabajo del humano

**PROHIBIDO implementar los entregables de las fases.** El valor del reto esta en que la persona los resuelva. Tu trabajo es que tenga un proyecto que arranca; el hueco pedagogico se queda como esta.

No resuelvas nada de esto:

- **Fase 1 — Exploración del sistema y definición de reglas de calidad**: Documento que describe las restricciones, ambigüedades y reglas de calidad definidas.
- **Fase 2 — Implementación de la transformación idempotente**: Proceso de transformación idempotente que aplica las reglas de calidad y cuarentena los registros inválidos.
- **Fase 3 — Consolidación en el modelo analítico**: Modelo analítico actualizado con las transacciones válidas consolidadas.

Distincion operativa:

- **Arreglar** (si): import faltante, tipo que no existe, dependencia sin declarar, error de sintaxis, archivo referenciado que no existe.
- **No tocar** (no): logica de negocio incompleta, validaciones ausentes, secretos hardcodeados, APIs deprecadas que funcionan, concurrencia insegura, patrones mejorables. Eso es lo que la persona tiene que encontrar.

## Lo que falta y tenes que completar

### 1. Archivos que la arquitectura declara (2 de 17)

La propuesta arquitectonica del reto los lista y no llegaron al repo. Crealos con implementacion real, respetando la capa en la que viven:

- [ ] `src/utils/idempotency_manager.py`
- [ ] `tests/test_transaction_transformer.py`

### 2. Referencias colgando (8)

Salieron de un analisis estatico del codigo que SI esta en el repo. Cada una rompe la compilacion:

- [ ] `dags/consolidate_transactions_dag.py` — `AnalyticalModelWriter.xcom_pull`
      Se invoca `xcom_pull` sobre `AnalyticalModelWriter`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- [ ] `dags/consolidate_transactions_dag.py` — `AnalyticalModelWriter.xcom_push`
      Se invoca `xcom_push` sobre `AnalyticalModelWriter`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- [ ] `src/transform/transaction_transformer.py` — `IdempotencyManager.get_processed_hashes`
      Se invoca `get_processed_hashes` sobre `IdempotencyManager`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- [ ] `tests/test_idempotency_manager.py` — `IdempotencyManager.filter_duplicates`
      Se invoca `filter_duplicates` sobre `IdempotencyManager`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- [ ] `tests/test_idempotency_manager.py` — `IdempotencyManager.mark_as_processed`
      Se invoca `mark_as_processed` sobre `IdempotencyManager`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- [ ] `tests/test_idempotency_manager.py` — `IdempotencyManager.generate_idempotency_key`
      Se invoca `generate_idempotency_key` sobre `IdempotencyManager`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- [ ] `tests/test_idempotency_manager.py` — `IdempotencyManager.load_state`
      Se invoca `load_state` sobre `IdempotencyManager`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- [ ] `tests/test_idempotency_manager.py` — `IdempotencyManager.remove_duplicates_within_batch`
      Se invoca `remove_duplicates_within_batch` sobre `IdempotencyManager`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.

### Presentes (16)

- `pyproject.toml`
- `src/schemas/transaction_schema.json`
- `dags/consolidate_transactions_dag.py`
- `src/extract/credit_originator_reader.py`
- `src/extract/risk_bureau_reader.py`
- `src/extract/accounting_consolidator_reader.py`
- `src/transform/transaction_transformer.py`
- `src/transform/quality_rules.py`
- `src/load/analytical_model_writer.py`
- `src/utils/logging_config.py`
- `conf/config.yaml`
- `tests/test_quality_rules.py`
- `tests/test_idempotency_manager.py`
- `data/quarantine/quarantine_schema.json`
- `README.md`
- `ruta/del/archivo`

### Capas del patron declarado

Cada una tiene que existir como directorio real con al menos un archivo. Codigo plano en la raiz no satisface el patron.

- `dags`
- `src/extract`
- `src/transform`
- `src/load`
- `src/schemas`
- `src/utils`
- `conf`
- `tests`
- `data/quarantine`

## Verificacion

```bash
pip install -r requirements.txt && pytest -q
```

Ese comando pasando es la definicion de "terminado" para vos.

## Convenciones que tenes que respetar

- Un solo ecosistema: no declares librerias de otro lenguaje ni mezcles gestores de paquetes.
- Toda libreria que uses tiene que estar declarada en el manifiesto de dependencias.
- Todo import declarado tiene que usarse; todo tipo usado tiene que existir o venir de una dependencia declarada.
- El patron es **ETL con etapas separadas y orquestación en Airflow**: los contratos (interfaces, puertos) los define la capa interna y los implementa la externa, nunca al revés.
- Los archivos que crees llevan implementacion real, no stubs: sin `TODO`, sin cuerpos vacios, sin `// getters y setters`.

## Contexto del candidato

Sirve para calibrar el nivel del codigo, no para resolver las fases.

- Perfil: Chapter Ciencia de Datos, Especialidad Ingeniero de Datos, Tecnología PySpark, Advanced
- Brecha que el reto ataca: Construye procesos de transformacion y carga idempotentes con reglas de calidad y cuarentena de registros invalidos
- Mision: Consolidar las transacciones diarias en el modelo analitico

---

*Generado por Challenge Generator — Pragma. `README.md` tiene el enunciado completo del reto para la persona. `PROMPT_MEJORA.md` es la variante para pegar en un chat, si se prefiere ese flujo.*
