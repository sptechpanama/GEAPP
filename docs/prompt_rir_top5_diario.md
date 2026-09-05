# Prompt diario para `RIR_TOP5_DIARIO`

Usa este texto en la conversación de ChatGPT que ya tiene acceso al archivo
`PC_Python`. Primero ejecútalo manualmente y revisa el resultado. Crea la tarea
programada únicamente cuando Rodrigo lo autorice expresamente.

> Revisa la hoja `RIR_INVESTIGACION_PROVEEDORES` del archivo `PC_Python` y
> actualiza el Top 5 ejecutivo de oportunidades vigentes para RIR en la hoja
> `RIR_TOP5_DIARIO`.
>
> Escribe exactamente estas columnas, en este orden:
> `fecha_corte`, `ranking`, `ficha`, `nombre_ficha`, `oportunidad`,
> `numero_acto`, `enlace_acto`, `fecha_cierre`, `numeros_preliminares`,
> `evaluacion_directa`, `accion_inmediata`, `proveedor_objetivo`,
> `recomendacion_general`, `estado`, `id_snapshot`, `actualizado_en`,
> `enlace_ficha_minsa`, `producto_recomendado`, `marca_producto`,
> `pais_origen`, `enlace_producto_recomendado`.
>
> Reglas obligatorias:
>
> 1. Publica exactamente los rankings 1 a 5. Si vuelves a ejecutar la tarea el
>    mismo día, reemplaza únicamente ese corte y conserva los cortes históricos.
> 2. Usa `fecha_corte|ranking|ficha|numero_acto` como `id_snapshot`. Marca
>    `estado` como `Vigente` solo después de terminar y validar las cinco filas.
> 3. Cada oportunidad debe tener tres enlaces HTTP(S) distintos y funcionales:
>    el acto oficial de Panamá Compra, la ficha oficial de CTNI/MINSA y la página
>    específica del producto recomendado.
> 4. Obtén `enlace_ficha_minsa` exclusivamente de la columna
>    `enlace_ficha_tecnica` del catálogo oficial `fichas_ctni_con_enlace`. No
>    construyas la URL ni supongas que el número de ficha equivale a `idficha`.
> 5. `enlace_producto_recomendado` debe abrir el producto o modelo exacto, no la
>    portada genérica del proveedor. Registra también el producto/modelo, la
>    marca y el país de origen verificado. Si un dato no puede verificarse,
>    escribe `No confirmado`; no lo inventes.
> 6. En `numeros_preliminares`, llama al benchmark exactamente
>    `Precio competitivo histórico (percentil 25 de ofertas unitarias comparables)`.
>    Llama al margen preliminar exactamente
>    `Diferencia bruta preliminar (precio competitivo histórico menos costo localizado, antes de flete, impuestos y otros gastos)`.
>    Si no existe benchmark histórico y comparas contra la referencia del acto,
>    indícalo de forma explícita en esa misma etiqueta.
> 7. Nunca presentes la diferencia bruta preliminar como utilidad o margen neto.
> 8. Antes de finalizar, comprueba que no haya rankings duplicados, campos
>    esenciales vacíos, enlaces genéricos, enlaces rotos ni actos vencidos.
> 9. Mantén recomendaciones breves, accionables y sustentadas. No sustituyas los
>    datos existentes con estimaciones no verificadas.

