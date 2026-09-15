# Oportunidades externas: registros y cobertura

## Dónde verlo

En **Oportunidades externas → Registros y accesos** se muestran Naturgy y AES con:

- Empresa: RS Engineering o SPTech.
- Estado guardado, pendiente para acceder y enlace oficial de registro.
- Pasos oficiales desplegables y notas compartidas.

El mismo cuadro está disponible en el desplegable de Naturgy y AES dentro de RS/SP.
Los cambios se guardan en `external_source_access` de Supabase. La clave es
`(source, company)` y cada edición comprueba la revisión anterior para impedir
que un usuario sobrescriba una modificación simultánea. La tabla tiene RLS;
se accede por la conexión PostgreSQL del servidor, tras la autenticación de la app.
Si falla la lectura, se indica **Sin consultar**, no se presenta el valor inicial
como dato confirmado y se deshabilita guardar hasta recuperar la conexión.

Registrar el avance no crea cuentas ni envía mensajes. Tampoco incorpora acceso
automático al contenido privado. Ambos canales requieren precalificación o
invitación y aún no aportan licitaciones privadas a la tabla de oportunidades.

## Fuentes oficiales verificadas el 15/09/2026

- Naturgy: https://www.naturgy.com.pa/proveedores/
- AES, proceso: https://www.aespanama.com/es/proveedores
- AES, alta inicial: https://www.aespanama.com/es/proveedores-potenciales

Para Ariba se abre el enlace desde AES; no se guarda una URL temporal de sesión.

## Cobertura

El scraper mantiene sus tres corridas diarias: 06:20, 12:20 y 18:20, hora de Panamá.
La página muestra la cantidad de fuentes de la última corrida; una captura manual
de un subconjunto no se presenta como si hubiese consultado todas.
**Fuentes y cobertura** diferencia captura completa, parcial, error y acceso
pendiente, y muestra el avance de lectura de detalles y adjuntos. Un listado
consultado no implica que todos sus documentos estén interpretados.

Actualizar vista vuelve a leer Supabase. Solicitar captura utiliza la cola del
orquestador existente. Ningún scraper corre dentro de Streamlit.
