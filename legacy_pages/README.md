# Páginas históricas

Esta carpeta conserva temporalmente páginas retiradas de la navegación de
Streamlit para evitar duplicidad sin perder capacidad de recuperación.

## Inteligencia CT

- `inteligencia_ct_proveedores.py`
- `inteligencia_ct_proveedores_flexible.py`

La página activa y unificada es
`pages/inteligencia_oportunidades_proveedores.py`.

## Panamá Compra

- `panama_compra2_0.py`: antigua variante de validación.

La página oficial es `pages/panama_compra.py`; el detector reutilizable sigue
disponible en `services/panama_compra_detection_v2.py`.

Los archivos de esta carpeta no son descubiertos automáticamente por
Streamlit. No deben volver a copiarse dentro de `pages/` sin revisar primero
qué funcionalidad concreta falta en la página unificada correspondiente.
