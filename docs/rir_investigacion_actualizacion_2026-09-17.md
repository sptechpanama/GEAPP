# Investigación RIR: selección vigente y flexible

## Resultado verificado el 17 de septiembre de 2026

- 85 investigaciones acumuladas; 14 candidatas vigentes con producto/proveedor
  localizado para cotizar o confirmar. Ninguna reúne todas las confirmaciones
  explícitas para presentarla como lista para ofertar.
- El Top externo más reciente era del 14 de septiembre; sus tres entradas habían
  vencido. La selección de Streamlit ahora se calcula desde la investigación y
  la captura actual de actos, aunque ese Top siga atrasado.
- Vista principal de diez candidatas con selector para verlas todas, situación,
  pendientes, próxima acción y enlaces. El orden prioriza preparación, prioridad
  editorial todavía respaldada y cierre próximo; no estima utilidades inexistentes.
- Se conservan la investigación, el Top y sus fechas originales en Sheets.

## Actualización y límites operativos

1. El orquestador mantiene los tres scrapers activos los siete días. Horarios de
   Panamá: CL abiertas a las 08:00, 09:00, 10:00, 11:00, 12:00, 13:00, 14:00 y
   17:00; programadas a las 02:30 y 18:00; licitaciones a las 16:30 y 18:40.
   La ampliación añade domingo a los horarios reales existentes de Sheets.
2. Configuración vigente: `FinanzasOperativas`, `pc_config!D2:D4`. El monitor del
   orquestador adoptó los siete días a las 15:42:56 sin reiniciar procesos.
   Los archivos locales de respaldo se alinearon con estos horarios.
3. Streamlit relee Sheets con caché de 60 segundos y recalcula la selección cada
   minuto mientras esta sección está abierta. Al abrirla, consulta los datos.
4. Una fila se retira cuando vence, incluso sin una nueva escritura en Sheets.
   La hora de Panamá se interpreta también en rangos CL y formatos AM/PM.
5. Capturas de más de 36 horas o sin fecha quedan por verificar. Las confirmaciones
   comerciales de más de 36 horas no permiten conservar la situación lista para
   ofertar. La página señala investigación y capturas atrasadas por separado.
6. Ante fallos de Sheets se conserva la última investigación leída correctamente
   con aviso. Si no se puede verificar la captura, no se afirma vigencia.
   Una pestaña con columnas incompletas no oculta las fuentes completas.
7. La computadora debe permanecer encendida, conectada y con el orquestador
   activo. La app no reemplaza al proceso externo de investigación de proveedores.
   No se modificó la tarea programada de ChatGPT: el prompt actualizado está en
   `docs/prompt_rir_top10_diario.md` y se descarga desde la propia sección.

## Reglas que permanecen

Solo fichas confirmadas sin CT ni RS. Mixtos únicamente por renglón o adjudicación
parcial. Se excluyen vencidos, fichas contradictorias, productos incompatibles,
actos globales cuyo alcance no está cubierto y opciones sin proveedor concreto.
Un precio, stock, documento o transporte pendiente permite evaluar; no confirma
cumplimiento, disponibilidad o ganancia. El monto total de un acto mixto nunca
se transforma en ingreso atribuible al renglón por esta selección.

Las versiones se concilian por acto, ficha y renglón. Un estudio nuevo no hereda
precios, márgenes ni cumplimiento del Top anterior. Si ambos tienen exactamente
la misma fecha de actualización, se conservan sus campos complementarios.

## Validación

86 pruebas automatizadas aprobadas: reglas regulatorias, fechas y horas, vencimiento
sin escritura, deduplicación por renglón, cambios de investigación, exclusiones,
confirmaciones, fallos y recuperación, Top antiguo e interacción real del
renderizador con Streamlit AppTest. Compilación de página y servicio correcta.
Comparación de solo lectura con las hojas reales: 14 candidatas para evaluar.
