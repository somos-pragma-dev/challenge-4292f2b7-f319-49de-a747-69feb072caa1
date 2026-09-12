# Consolidación de transacciones diarias en el modelo analítico

La empresa de finanzas requiere consolidar las transacciones diarias en un modelo analítico. El proceso debe ser idempotente y manejar reglas de calidad, incluyendo la cuarentena de registros inválidos. Los actores involucrados son el originador de créditos, el buró de riesgos y el consolidador contable. El sistema debe procesar hasta 10 000 transacciones por minuto con una latencia máxima de 5 segundos por transacción. La idempotencia se garantiza mediante un identificador único de transacción y se debe mantener la consistencia entre el registro de la transacción y su consolidación en el modelo analítico.

## Informacion General

| Campo | Valor |
|-------|-------|
| **Tema** | pipelines de transformacion y carga |
| **Nivel** | advanced-l2 |
| **Tipo** | practical |
| **Tiempo estimado** | 10 horas |

## Fases del Reto

### Fase 0: Configuración del Proyecto

**Objetivo:** Obtener el proyecto base funcional enviando el Código Base a un asistente de IA, que lo analizará, corregirá errores y generará un ZIP listo para usar.

**Tiempo estimado:** 15-30 minutos

**Instrucciones:**

- Asegúrate de tener instalado para ejecutar el proyecto: Python 3.10+, pip, VS Code o similar.
- Copia todo el contenido del campo **Código Base** de este reto — incluyendo el texto de instrucciones que aparece al inicio.
- Abre un asistente de IA (Claude en claude.ai, ChatGPT o Gemini — se recomienda Claude), pega el contenido copiado en el chat y envíalo.
- El asistente analizará los archivos, corregirá errores y generará un archivo ZIP descargable. Descárgalo y extráelo en la carpeta donde quieras trabajar.
- Ejecuta `pip install -r requirements.txt` y luego arranca el proyecto. Si no hay errores, estás listo.

**Entregable:** El proyecto compila/arranca sin errores.

<details>
<summary>Pistas de conocimiento</summary>

- Copia el Código Base completo incluyendo el texto de instrucciones al inicio — esas instrucciones le indican al asistente exactamente qué hacer con los archivos.
- Si el asistente no genera el ZIP automáticamente al terminar el análisis, escríbele: "genera el ZIP ahora".
- Si el proyecto tiene errores al arrancar, comparte el mensaje de error con el mismo asistente para que lo corrija.

</details>

### Fase 1: Exploración del sistema y definición de reglas de calidad

**Objetivo:** Identificar las restricciones y ambigüedades del sistema y definir las reglas de calidad para las transacciones.

**Tiempo estimado:** 2 horas

**Instrucciones:**

- Explora el sistema existente para identificar las restricciones y ambigüedades.
- Define las reglas de calidad que deben cumplir las transacciones para ser consideradas válidas.

**Entregable:** Documento que describe las restricciones, ambigüedades y reglas de calidad definidas.

<details>
<summary>Pistas de conocimiento</summary>

- Considera las posibles fuentes de transacciones y sus características.
- Piensa en cómo garantizar la idempotencia en el proceso.

</details>

### Fase 2: Implementación de la transformación idempotente

**Objetivo:** Implementar la transformación de las transacciones de manera idempotente y aplicar las reglas de calidad definidas.

**Tiempo estimado:** 4 horas

**Instrucciones:**

- Implementa la transformación de las transacciones de manera idempotente.
- Aplica las reglas de calidad definidas en la fase anterior para filtrar las transacciones inválidas.

**Entregable:** Proceso de transformación idempotente que aplica las reglas de calidad y cuarentena los registros inválidos.

<details>
<summary>Pistas de conocimiento</summary>

- Utiliza un identificador único para garantizar la idempotencia.
- Considera cómo manejar los registros inválidos sin interrumpir el proceso.

</details>

### Fase 3: Consolidación en el modelo analítico

**Objetivo:** Consolidar las transacciones válidas en el modelo analítico.

**Tiempo estimado:** 4 horas

**Instrucciones:**

- Consolida las transacciones válidas en el modelo analítico.
- Asegura que el proceso sea idempotente y que se mantenga la consistencia entre el registro de la transacción y su consolidación.

**Entregable:** Modelo analítico actualizado con las transacciones válidas consolidadas.

<details>
<summary>Pistas de conocimiento</summary>

- Verifica que el proceso de consolidación sea idempotente.
- Asegura que se mantenga la consistencia entre el registro de la transacción y su consolidación en el modelo analítico.

</details>

## Dimensiones Evaluadas

- **queEs**: ¿Qué es la idempotencia en el contexto de este reto?
- **paraQueSirve**: ¿Para qué sirven las reglas de calidad en este proceso?
- **comoSeUsa**: ¿Cómo se usa un identificador único para garantizar la idempotencia?
- **erroresComunes**: ¿Cuáles son los errores comunes que pueden ocurrir durante la transformación y consolidación de transacciones?
- **queDecisionesImplica**: ¿Qué decisiones implica el manejo de registros inválidos en este proceso?

## Criterios de Evaluacion

- Implementación de la transformación idempotente.
- Aplicación de reglas de calidad y cuarentena de registros inválidos.
- Consolidación de transacciones válidas en el modelo analítico.
- Garantía de idempotencia y consistencia en el proceso.

## Como trabajar con un asistente de IA

Hay dos caminos, elegi uno:

- **AGENTS.md** (recomendado) — instrucciones nativas del repo. Abri esta carpeta con tu agente local (Claude Code, Cursor, Codex, Copilot, Gemini) y las carga solo. Sabe que archivos faltan y con que comando se verifica, y completa el scaffold escribiendo en disco.
- **PROMPT_MEJORA.md** — para copiar y pegar en un chat (claude.ai, ChatGPT). Devuelve un ZIP con el proyecto. Sirve si no tenes un agente en el IDE.

Ninguno de los dos resuelve las fases del reto: eso es tu trabajo.

## Verificacion

El proyecto esta listo para trabajar cuando este comando corre sin errores:

```bash
pip install -r requirements.txt && pytest -q
```

---

*Reto generado automaticamente por Challenge Generator - Pragma*
