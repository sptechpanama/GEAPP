from __future__ import annotations

"""Dominio y persistencia del Pipeline Estrategico de RIR Medical.

Supabase/PostgreSQL es la fuente de verdad.  SQLite se admite como respaldo de
desarrollo y para las pruebas automatizadas.  Toda mutacion genera una entrada
de auditoria y una entrada de outbox para la replica idempotente a Google
Sheets.
"""

from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import re
import unicodedata
import uuid
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import Engine, create_engine, text


STANDARD_CHECKLIST: tuple[tuple[str, str], ...] = (
    ("proveedor_contactado", "Proveedor Contactado"),
    ("cotizacion_y_fichas", "Proveedor ya envió cotizaciones y fichas"),
    (
        "cumple_especificaciones_documentacion_precio",
        "Proveedor cumple con especificaciones, documentación y precio",
    ),
    (
        "esperando_documentacion",
        "Ya esperando Documentación de parte del proveedor",
    ),
    (
        "documentacion_enviada_ct",
        "Documentación completa enviada por el proveedor para CT",
    ),
    ("cita_ct", "Cita en CSS asignada para tramitar CT"),
    ("expediente_ct", "Expediente para CT ingresado en CSS"),
    ("ct_aprobado", "CT aprobado y listo Para Licitar"),
    (
        "participacion_orden_refrendada",
        "Participación y orden de compra refrendada",
    ),
    (
        "entrega_recibido_conforme",
        "Entrega y recibido conforme exitoso en Licitación",
    ),
)

EXTENDED_SUFFIX: tuple[tuple[str, str], ...] = (
    ("proveedor_contactado", "Proveedores contactados"),
    ("cotizacion_y_fichas", "Proveedor ya envió cotización y fichas"),
    ("asistencia_homologacion", "Asistencia a homologación"),
    ("ficha_creada", "Ficha se creó"),
    (
        "cumple_especificaciones_documentacion",
        "Proveedor cumple con especificaciones y documentación",
    ),
    (
        "esperando_documentacion",
        "Esperando documentación de parte del proveedor",
    ),
    (
        "documentacion_enviada_ct",
        "Documentación completa enviada por parte del proveedor para CT",
    ),
    (
        "cita_ct",
        "Cita en CT asignada para tramitar Criterio Técnico",
    ),
    ("expediente_ct", "Expediente para CT ingresado en CSS"),
    ("ct_aprobado", "CT aprobado y listo para licitar"),
    (
        "participacion_orden_refrendada",
        "Participación y orden de compra refrendada",
    ),
    (
        "entrega_recibido_conforme",
        "Entrega y recibido conforme exitoso en Licitación",
    ),
)


@dataclass(frozen=True)
class RouteDefinition:
    key: str
    label: str
    short_label: str
    description: str
    accent: str
    checklist: tuple[tuple[str, str], ...]


ROUTES: dict[str, RouteDefinition] = {
    "fichas_viejas": RouteDefinition(
        key="fichas_viejas",
        label="Fichas viejas",
        short_label="Fichas viejas",
        description="Rescate de oportunidades del histórico.",
        accent="#f43f5e",
        checklist=STANDARD_CHECKLIST,
    ),
    "fichas_recien_creadas": RouteDefinition(
        key="fichas_recien_creadas",
        label="Fichas recién creadas",
        short_label="Recién creadas",
        description="Oportunidades jóvenes del mercado.",
        accent="#f59e0b",
        checklist=STANDARD_CHECKLIST,
    ),
    "homologaciones_anunciadas": RouteDefinition(
        key="homologaciones_anunciadas",
        label="Homologaciones anunciadas",
        short_label="Homologaciones",
        description="Alertas CTI con fecha de homologación.",
        accent="#3b82f6",
        checklist=(
            ("citas_homologacion_agregadas", "Citas (fechas de homologación) agregadas"),
        )
        + EXTENDED_SUFFIX,
    ),
    "solicitudes_creacion": RouteDefinition(
        key="solicitudes_creacion",
        label="Solicitudes de creación de ficha",
        short_label="Solicitudes",
        description="Solicitudes iniciadas por terceros.",
        accent="#10b981",
        checklist=(
            (
                "solicitudes_creacion_agregadas",
                "Solicitudes de creación de ficha agregadas",
            ),
        )
        + EXTENDED_SUFFIX,
    ),
    "creacion_desde_cero": RouteDefinition(
        key="creacion_desde_cero",
        label="Creación de fichas desde cero",
        short_label="Desde cero",
        description="Diseño de oportunidades nuevas.",
        accent="#8b5cf6",
        checklist=(
            (
                "fichas_a_crear_agregadas",
                "Fichas a crear agregadas o desde cero",
            ),
        )
        + EXTENDED_SUFFIX,
    ),
}


CARD_COLUMNS = (
    "id",
    "identity_key",
    "ficha",
    "nombre_ficha",
    "producto",
    "proveedor",
    "proveedor_norm",
    "marca",
    "marca_norm",
    "descripcion",
    "route_key",
    "estado",
    "responsable",
    "prioridad",
    "fecha_objetivo",
    "archived",
    "source",
    "source_external_id",
    "created_by",
    "updated_by",
    "created_at",
    "updated_at",
    "version",
)

CHECKPOINT_COLUMNS = (
    "id",
    "card_id",
    "checkpoint_key",
    "position",
    "label",
    "completed",
    "completed_at",
    "completed_by",
    "notes",
    "updated_at",
)

CONTACT_COLUMNS = (
    "id",
    "card_id",
    "nombre",
    "cargo",
    "email",
    "telefono",
    "whatsapp_wechat",
    "pais",
    "canal_preferido",
    "notas",
    "es_principal",
    "archived",
    "created_by",
    "created_at",
    "updated_at",
)

DOCUMENT_COLUMNS = (
    "id",
    "card_id",
    "storage_provider",
    "drive_file_id",
    "file_url",
    "file_name",
    "document_type",
    "mime_type",
    "size_bytes",
    "descripcion",
    "archived",
    "uploaded_by",
    "uploaded_at",
)

ACTIVITY_COLUMNS = (
    "id",
    "card_id",
    "action",
    "field_name",
    "old_value",
    "new_value",
    "actor",
    "created_at",
)

MIRROR_COLUMNS: dict[str, tuple[str, ...]] = {
    "card": CARD_COLUMNS,
    "checkpoint": CHECKPOINT_COLUMNS,
    "contact": CONTACT_COLUMNS,
    "document": DOCUMENT_COLUMNS,
    "activity": ACTIVITY_COLUMNS,
}


class PipelineError(RuntimeError):
    """Error funcional legible para la interfaz."""


class PipelineRuleError(PipelineError):
    def __init__(self, message: str, *, requires_confirmation: bool = False):
        super().__init__(message)
        self.requires_confirmation = requires_confirmation


@dataclass(frozen=True)
class PipelineFilters:
    routes: tuple[str, ...] = ()
    providers: tuple[str, ...] = ()
    fichas: tuple[str, ...] = ()
    states: tuple[str, ...] = ()
    search: str = ""
    include_archived: bool = False


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_key(value: Any) -> str:
    raw = unicodedata.normalize("NFKD", clean_text(value))
    raw = "".join(char for char in raw if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", " ", raw.lower()).strip()


def normalize_ficha(value: Any) -> str:
    raw = clean_text(value).replace("*", "")
    match = re.search(r"\d+", raw)
    return match.group(0) if match else raw.upper()


def identity_key(
    *, ficha: Any, producto: Any, proveedor: Any, marca: Any
) -> str:
    ficha_key = normalize_ficha(ficha)
    if not ficha_key:
        product_key = normalize_key(producto)
        if not product_key:
            raise PipelineError(
                "Indica una ficha técnica o un producto provisional para identificar la tarjeta."
            )
        ficha_key = f"POR-CREAR:{product_key}"
    provider_key = normalize_key(proveedor)
    brand_key = normalize_key(marca)
    if not provider_key:
        raise PipelineError("El proveedor es obligatorio.")
    if not brand_key:
        raise PipelineError("La marca es obligatoria.")
    return "|".join((ficha_key, provider_key, brand_key))


COPY_IDENTITY_MARKER = "|COPIA:"


def _copy_identity_suffix(value: Any) -> str:
    """Conserva la identidad interna de una copia al editar sus datos."""
    raw = clean_text(value)
    marker_position = raw.find(COPY_IDENTITY_MARKER)
    return raw[marker_position:] if marker_position >= 0 else ""


def _row_dict(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    mapping = getattr(row, "_mapping", row)
    return dict(mapping)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "si", "sí"}


class PipelineRepository:
    """Repositorio transaccional del pipeline."""

    def __init__(self, engine: Engine, *, source_label: str = "database") -> None:
        self.engine = engine
        self.source_label = source_label
        self.ensure_schema()

    @classmethod
    def connect(
        cls,
        database_url: str | None = None,
        *,
        local_path: str | Path | None = None,
    ) -> "PipelineRepository":
        if local_path is not None and not clean_text(database_url):
            path = Path(local_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            return cls(
                create_engine(f"sqlite:///{path.as_posix()}", pool_pre_ping=True),
                source_label=f"SQLite local ({path})",
            )
        url = clean_text(
            database_url
            or os.getenv("PIPELINE_DB_URL")
            or os.getenv("SUPABASE_DB_URL")
            or os.getenv("DATABASE_URL")
        )
        if url:
            kwargs: dict[str, Any] = {"pool_pre_ping": True, "pool_recycle": 240}
            if url.startswith("postgresql"):
                kwargs["connect_args"] = {"connect_timeout": 12}
            engine = create_engine(url, **kwargs)
            return cls(engine, source_label="Supabase (PostgreSQL)")

        path = Path(local_path or Path(__file__).resolve().parents[1] / "data" / "pipeline_estrategico.db")
        path.parent.mkdir(parents=True, exist_ok=True)
        return cls(
            create_engine(f"sqlite:///{path.as_posix()}", pool_pre_ping=True),
            source_label=f"SQLite local ({path})",
        )

    @property
    def dialect(self) -> str:
        return self.engine.dialect.name

    def close(self) -> None:
        self.engine.dispose()

    def ensure_schema(self) -> None:
        statements = [
            """
            CREATE TABLE IF NOT EXISTS pipeline_cards (
                id TEXT PRIMARY KEY,
                identity_key TEXT NOT NULL UNIQUE,
                ficha TEXT NOT NULL DEFAULT '',
                nombre_ficha TEXT NOT NULL DEFAULT '',
                producto TEXT NOT NULL DEFAULT '',
                proveedor TEXT NOT NULL,
                proveedor_norm TEXT NOT NULL,
                marca TEXT NOT NULL,
                marca_norm TEXT NOT NULL,
                descripcion TEXT NOT NULL DEFAULT '',
                route_key TEXT NOT NULL,
                estado TEXT NOT NULL DEFAULT 'activo',
                responsable TEXT NOT NULL DEFAULT '',
                prioridad INTEGER NOT NULL DEFAULT 3,
                fecha_objetivo TEXT NOT NULL DEFAULT '',
                archived INTEGER NOT NULL DEFAULT 0,
                source TEXT NOT NULL DEFAULT 'manual',
                source_external_id TEXT NOT NULL DEFAULT '',
                created_by TEXT NOT NULL DEFAULT '',
                updated_by TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
                id TEXT PRIMARY KEY,
                card_id TEXT NOT NULL,
                checkpoint_key TEXT NOT NULL,
                position INTEGER NOT NULL,
                label TEXT NOT NULL,
                completed INTEGER NOT NULL DEFAULT 0,
                completed_at TEXT NOT NULL DEFAULT '',
                completed_by TEXT NOT NULL DEFAULT '',
                notes TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL,
                UNIQUE(card_id, checkpoint_key),
                FOREIGN KEY(card_id) REFERENCES pipeline_cards(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS pipeline_contacts (
                id TEXT PRIMARY KEY,
                card_id TEXT NOT NULL,
                nombre TEXT NOT NULL DEFAULT '',
                cargo TEXT NOT NULL DEFAULT '',
                email TEXT NOT NULL DEFAULT '',
                telefono TEXT NOT NULL DEFAULT '',
                whatsapp_wechat TEXT NOT NULL DEFAULT '',
                pais TEXT NOT NULL DEFAULT '',
                canal_preferido TEXT NOT NULL DEFAULT '',
                notas TEXT NOT NULL DEFAULT '',
                es_principal INTEGER NOT NULL DEFAULT 0,
                archived INTEGER NOT NULL DEFAULT 0,
                created_by TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(card_id) REFERENCES pipeline_cards(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS pipeline_documents (
                id TEXT PRIMARY KEY,
                card_id TEXT NOT NULL,
                storage_provider TEXT NOT NULL DEFAULT 'drive',
                drive_file_id TEXT NOT NULL DEFAULT '',
                file_url TEXT NOT NULL DEFAULT '',
                file_name TEXT NOT NULL,
                document_type TEXT NOT NULL DEFAULT '',
                mime_type TEXT NOT NULL DEFAULT '',
                size_bytes INTEGER NOT NULL DEFAULT 0,
                descripcion TEXT NOT NULL DEFAULT '',
                archived INTEGER NOT NULL DEFAULT 0,
                uploaded_by TEXT NOT NULL DEFAULT '',
                uploaded_at TEXT NOT NULL,
                FOREIGN KEY(card_id) REFERENCES pipeline_cards(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS pipeline_activity (
                id TEXT PRIMARY KEY,
                card_id TEXT NOT NULL,
                action TEXT NOT NULL,
                field_name TEXT NOT NULL DEFAULT '',
                old_value TEXT NOT NULL DEFAULT '',
                new_value TEXT NOT NULL DEFAULT '',
                actor TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                FOREIGN KEY(card_id) REFERENCES pipeline_cards(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS pipeline_sync_outbox (
                id TEXT PRIMARY KEY,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                operation TEXT NOT NULL,
                payload TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                synced_at TEXT NOT NULL DEFAULT ''
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_pipeline_cards_route ON pipeline_cards(route_key, archived)",
            "CREATE INDEX IF NOT EXISTS idx_pipeline_cards_provider ON pipeline_cards(proveedor_norm, archived)",
            "CREATE INDEX IF NOT EXISTS idx_pipeline_cards_ficha ON pipeline_cards(ficha, archived)",
            """CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_cards_external
            ON pipeline_cards(source, source_external_id) WHERE source_external_id <> ''""",
            "CREATE INDEX IF NOT EXISTS idx_pipeline_checkpoints_card ON pipeline_checkpoints(card_id, position)",
            "CREATE INDEX IF NOT EXISTS idx_pipeline_contacts_card ON pipeline_contacts(card_id, archived)",
            "CREATE INDEX IF NOT EXISTS idx_pipeline_documents_card ON pipeline_documents(card_id, archived)",
            "CREATE INDEX IF NOT EXISTS idx_pipeline_activity_card ON pipeline_activity(card_id, created_at)",
            "CREATE INDEX IF NOT EXISTS idx_pipeline_outbox_status ON pipeline_sync_outbox(status, created_at)",
        ]
        with self.engine.begin() as connection:
            for statement in statements:
                connection.execute(text(statement))

    def _audit(
        self,
        connection,
        *,
        card_id: str,
        action: str,
        actor: str,
        field_name: str = "",
        old_value: Any = "",
        new_value: Any = "",
    ) -> dict[str, Any]:
        row = {
            "id": uuid.uuid4().hex,
            "card_id": card_id,
            "action": action,
            "field_name": field_name,
            "old_value": _json(old_value) if not isinstance(old_value, str) else old_value,
            "new_value": _json(new_value) if not isinstance(new_value, str) else new_value,
            "actor": clean_text(actor),
            "created_at": utc_now(),
        }
        connection.execute(
            text(
                """INSERT INTO pipeline_activity
                (id,card_id,action,field_name,old_value,new_value,actor,created_at)
                VALUES (:id,:card_id,:action,:field_name,:old_value,:new_value,:actor,:created_at)"""
            ),
            row,
        )
        self._queue_outbox(connection, "activity", row["id"], "upsert", row)
        return row

    def _queue_outbox(
        self,
        connection,
        entity_type: str,
        entity_id: str,
        operation: str,
        payload: Mapping[str, Any],
    ) -> None:
        now = utc_now()
        connection.execute(
            text(
                """INSERT INTO pipeline_sync_outbox
                (id,entity_type,entity_id,operation,payload,status,attempts,last_error,created_at,updated_at,synced_at)
                VALUES (:id,:entity_type,:entity_id,:operation,:payload,'pending',0,'',:created_at,:updated_at,'')"""
            ),
            {
                "id": uuid.uuid4().hex,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "operation": operation,
                "payload": _json(dict(payload)),
                "created_at": now,
                "updated_at": now,
            },
        )

    def create_card(
        self,
        *,
        ficha: Any,
        nombre_ficha: Any,
        producto: Any,
        proveedor: Any,
        marca: Any,
        descripcion: Any,
        route_key: str,
        actor: str,
        responsable: str = "",
        prioridad: int = 3,
        fecha_objetivo: Any = "",
        source: str = "manual",
        source_external_id: str = "",
    ) -> dict[str, Any]:
        if route_key not in ROUTES:
            raise PipelineError("La categoría seleccionada no existe.")
        provider = clean_text(proveedor)
        brand = clean_text(marca)
        product = clean_text(producto)
        ficha_value = normalize_ficha(ficha)
        key = identity_key(
            ficha=ficha_value,
            producto=product,
            proveedor=provider,
            marca=brand,
        )
        now = utc_now()
        row = {
            "id": uuid.uuid4().hex,
            "identity_key": key,
            "ficha": ficha_value,
            "nombre_ficha": clean_text(nombre_ficha),
            "producto": product,
            "proveedor": provider,
            "proveedor_norm": normalize_key(provider),
            "marca": brand,
            "marca_norm": normalize_key(brand),
            "descripcion": clean_text(descripcion),
            "route_key": route_key,
            "estado": "activo",
            "responsable": clean_text(responsable),
            "prioridad": max(1, min(5, int(prioridad or 3))),
            "fecha_objetivo": self._date_string(fecha_objetivo),
            "archived": 0,
            "source": clean_text(source) or "manual",
            "source_external_id": clean_text(source_external_id),
            "created_by": clean_text(actor),
            "updated_by": clean_text(actor),
            "created_at": now,
            "updated_at": now,
            "version": 1,
        }
        try:
            with self.engine.begin() as connection:
                if row["source_external_id"]:
                    existing = connection.execute(
                        text(
                            "SELECT * FROM pipeline_cards WHERE source=:source AND source_external_id=:external LIMIT 1"
                        ),
                        {"source": row["source"], "external": row["source_external_id"]},
                    ).first()
                    if existing:
                        return _row_dict(existing)
                existing = connection.execute(
                    text("SELECT id FROM pipeline_cards WHERE identity_key=:key LIMIT 1"),
                    {"key": key},
                ).first()
                if existing:
                    raise PipelineError(
                        "Ya existe una tarjeta para esa combinacion de ficha, proveedor y marca."
                    )
                columns = ",".join(CARD_COLUMNS)
                values = ",".join(f":{column}" for column in CARD_COLUMNS)
                connection.execute(
                    text(f"INSERT INTO pipeline_cards ({columns}) VALUES ({values})"), row
                )
                checkpoint_rows: list[dict[str, Any]] = []
                for position, (checkpoint_key, label) in enumerate(
                    ROUTES[route_key].checklist, start=1
                ):
                    checkpoint = {
                        "id": uuid.uuid4().hex,
                        "card_id": row["id"],
                        "checkpoint_key": checkpoint_key,
                        "position": position,
                        "label": label,
                        "completed": 0,
                        "completed_at": "",
                        "completed_by": "",
                        "notes": "",
                        "updated_at": now,
                    }
                    checkpoint_rows.append(checkpoint)
                    cp_columns = ",".join(CHECKPOINT_COLUMNS)
                    cp_values = ",".join(f":{column}" for column in CHECKPOINT_COLUMNS)
                    connection.execute(
                        text(
                            f"INSERT INTO pipeline_checkpoints ({cp_columns}) VALUES ({cp_values})"
                        ),
                        checkpoint,
                    )
                    self._queue_outbox(
                        connection, "checkpoint", checkpoint["id"], "upsert", checkpoint
                    )
                self._queue_outbox(connection, "card", row["id"], "upsert", row)
                self._audit(
                    connection,
                    card_id=row["id"],
                    action="card_created",
                    actor=actor,
                    new_value={"route_key": route_key, "identity_key": key},
                )
        except PipelineError:
            raise
        except Exception as exc:
            if "unique" in str(exc).lower():
                raise PipelineError(
                    "Ya existe una tarjeta para esa combinacion de ficha, proveedor y marca."
                ) from exc
            raise
        return self.get_card(row["id"])

    @staticmethod
    def _date_string(value: Any) -> str:
        if isinstance(value, (datetime, date)):
            return value.date().isoformat() if isinstance(value, datetime) else value.isoformat()
        return clean_text(value)

    def get_card(self, card_id: str) -> dict[str, Any]:
        with self.engine.connect() as connection:
            row = connection.execute(
                text("SELECT * FROM pipeline_cards WHERE id=:id LIMIT 1"), {"id": card_id}
            ).first()
        if not row:
            raise PipelineError("La tarjeta ya no existe.")
        return _row_dict(row)

    def card_by_source(self, source: str, external_id: str) -> dict[str, Any]:
        source_value = clean_text(source)
        external_value = clean_text(external_id)
        if not source_value or not external_value:
            return {}
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    """SELECT * FROM pipeline_cards
                    WHERE source=:source AND source_external_id=:external LIMIT 1"""
                ),
                {"source": source_value, "external": external_value},
            ).first()
        return _row_dict(row)

    def card_by_identity(
        self,
        *,
        ficha: Any,
        producto: Any,
        proveedor: Any,
        marca: Any,
    ) -> dict[str, Any]:
        key = identity_key(
            ficha=ficha,
            producto=producto,
            proveedor=proveedor,
            marca=marca,
        )
        with self.engine.connect() as connection:
            row = connection.execute(
                text("SELECT * FROM pipeline_cards WHERE identity_key=:key LIMIT 1"),
                {"key": key},
            ).first()
        return _row_dict(row)

    def list_cards(self, filters: PipelineFilters | None = None) -> list[dict[str, Any]]:
        selected = filters or PipelineFilters()
        conditions = ["1=1"]
        params: dict[str, Any] = {}
        if not selected.include_archived:
            conditions.append("c.archived=0")
        if selected.routes:
            placeholders = []
            for index, value in enumerate(selected.routes):
                key = f"route_{index}"
                placeholders.append(f":{key}")
                params[key] = value
            conditions.append(f"c.route_key IN ({','.join(placeholders)})")
        if selected.states:
            placeholders = []
            for index, value in enumerate(selected.states):
                key = f"state_{index}"
                placeholders.append(f":{key}")
                params[key] = value
            conditions.append(f"c.estado IN ({','.join(placeholders)})")
        query = f"""
            SELECT c.*,
                   COALESCE(SUM(CASE WHEN cp.completed=1 THEN 1 ELSE 0 END),0) AS completed_steps,
                   COUNT(cp.id) AS total_steps
            FROM pipeline_cards c
            LEFT JOIN pipeline_checkpoints cp ON cp.card_id=c.id
            WHERE {' AND '.join(conditions)}
            GROUP BY {','.join('c.' + column for column in CARD_COLUMNS)}
            ORDER BY c.prioridad ASC, c.updated_at DESC
        """
        with self.engine.connect() as connection:
            rows = [_row_dict(row) for row in connection.execute(text(query), params)]
        provider_set = {normalize_key(value) for value in selected.providers if clean_text(value)}
        ficha_set = {normalize_ficha(value) for value in selected.fichas if clean_text(value)}
        search = normalize_key(selected.search)
        output: list[dict[str, Any]] = []
        for row in rows:
            if provider_set and row.get("proveedor_norm", "") not in provider_set:
                continue
            if ficha_set and normalize_ficha(row.get("ficha")) not in ficha_set:
                continue
            haystack = normalize_key(
                " ".join(
                    str(row.get(column, "") or "")
                    for column in (
                        "ficha",
                        "nombre_ficha",
                        "producto",
                        "proveedor",
                        "marca",
                        "descripcion",
                        "responsable",
                    )
                )
            )
            if search and search not in haystack:
                continue
            completed = int(row.get("completed_steps") or 0)
            total = int(row.get("total_steps") or 0)
            row["progress"] = round((completed / total * 100.0) if total else 0.0, 1)
            output.append(row)
        return output

    def options(self) -> dict[str, list[str]]:
        cards = self.list_cards(PipelineFilters(include_archived=False))
        return {
            "providers": sorted({clean_text(card.get("proveedor")) for card in cards if clean_text(card.get("proveedor"))}),
            "fichas": sorted({normalize_ficha(card.get("ficha")) for card in cards if normalize_ficha(card.get("ficha"))}, key=lambda value: (not value.isdigit(), int(value) if value.isdigit() else value)),
            "states": sorted({clean_text(card.get("estado")) for card in cards if clean_text(card.get("estado"))}),
        }

    def checkpoints(self, card_id: str) -> list[dict[str, Any]]:
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    "SELECT * FROM pipeline_checkpoints WHERE card_id=:card ORDER BY position"
                ),
                {"card": card_id},
            )
            return [_row_dict(row) for row in rows]

    def set_checkpoint(
        self,
        *,
        card_id: str,
        checkpoint_key: str,
        completed: bool,
        actor: str,
        reset_downstream: bool = False,
    ) -> list[dict[str, Any]]:
        now = utc_now()
        with self.engine.begin() as connection:
            rows = [
                _row_dict(row)
                for row in connection.execute(
                    text(
                        "SELECT * FROM pipeline_checkpoints WHERE card_id=:card ORDER BY position"
                    ),
                    {"card": card_id},
                )
            ]
            target_index = next(
                (index for index, row in enumerate(rows) if row["checkpoint_key"] == checkpoint_key),
                None,
            )
            if target_index is None:
                raise PipelineRuleError("El punto de control no pertenece a esta tarjeta.")
            target = rows[target_index]
            current = _as_bool(target.get("completed"))
            if current == bool(completed):
                return rows
            if completed:
                missing = [row for row in rows[:target_index] if not _as_bool(row.get("completed"))]
                if missing:
                    raise PipelineRuleError(
                        f"Completa primero: {missing[0]['label']}."
                    )
                affected = [target]
            else:
                downstream = [row for row in rows[target_index + 1 :] if _as_bool(row.get("completed"))]
                if downstream and not reset_downstream:
                    raise PipelineRuleError(
                        "Este cambio reiniciara tambien los controles posteriores ya completados.",
                        requires_confirmation=True,
                    )
                affected = rows[target_index:]

            for row in affected:
                new_completed = bool(completed) if row["id"] == target["id"] else False
                updated = dict(row)
                updated.update(
                    {
                        "completed": 1 if new_completed else 0,
                        "completed_at": now if new_completed else "",
                        "completed_by": clean_text(actor) if new_completed else "",
                        "updated_at": now,
                    }
                )
                connection.execute(
                    text(
                        """UPDATE pipeline_checkpoints
                        SET completed=:completed,completed_at=:completed_at,
                            completed_by=:completed_by,updated_at=:updated_at
                        WHERE id=:id"""
                    ),
                    updated,
                )
                self._queue_outbox(connection, "checkpoint", row["id"], "upsert", updated)
            connection.execute(
                text(
                    """UPDATE pipeline_cards SET updated_at=:now,updated_by=:actor,
                    version=version+1 WHERE id=:id"""
                ),
                {"now": now, "actor": clean_text(actor), "id": card_id},
            )
            card = _row_dict(
                connection.execute(
                    text("SELECT * FROM pipeline_cards WHERE id=:id"), {"id": card_id}
                ).first()
            )
            self._queue_outbox(connection, "card", card_id, "upsert", card)
            self._audit(
                connection,
                card_id=card_id,
                action="checkpoint_completed" if completed else "checkpoint_reopened",
                actor=actor,
                field_name=checkpoint_key,
                old_value=current,
                new_value=bool(completed),
            )
        return self.checkpoints(card_id)

    def update_card(
        self,
        card_id: str,
        *,
        actor: str,
        expected_version: int | None = None,
        **changes: Any,
    ) -> dict[str, Any]:
        allowed = {
            "ficha",
            "nombre_ficha",
            "producto",
            "proveedor",
            "marca",
            "descripcion",
            "estado",
            "responsable",
            "prioridad",
            "fecha_objetivo",
        }
        incoming = {key: value for key, value in changes.items() if key in allowed}
        if not incoming:
            return self.get_card(card_id)
        old = self.get_card(card_id)
        current_version = int(old.get("version") or 1)
        if expected_version is not None and int(expected_version) != current_version:
            raise PipelineError(
                "La tarjeta fue modificada por otro usuario. Recarga la página antes de guardar."
            )
        merged = dict(old)
        merged.update(incoming)
        merged["ficha"] = normalize_ficha(merged.get("ficha"))
        merged["proveedor"] = clean_text(merged.get("proveedor"))
        merged["marca"] = clean_text(merged.get("marca"))
        merged["producto"] = clean_text(merged.get("producto"))
        canonical_identity = identity_key(
            ficha=merged.get("ficha"),
            producto=merged.get("producto"),
            proveedor=merged.get("proveedor"),
            marca=merged.get("marca"),
        )
        # Una tarjeta duplicada puede mantener exactamente la misma ficha,
        # proveedor y marca que la original. El sufijo es interno y no altera
        # ninguno de los campos visibles para el usuario.
        merged["identity_key"] = canonical_identity + _copy_identity_suffix(
            old.get("identity_key")
        )
        merged["proveedor_norm"] = normalize_key(merged["proveedor"])
        merged["marca_norm"] = normalize_key(merged["marca"])
        merged["nombre_ficha"] = clean_text(merged.get("nombre_ficha"))
        merged["descripcion"] = clean_text(merged.get("descripcion"))
        merged["estado"] = clean_text(merged.get("estado")) or "activo"
        merged["responsable"] = clean_text(merged.get("responsable"))
        merged["prioridad"] = max(1, min(5, int(merged.get("prioridad") or 3)))
        merged["fecha_objetivo"] = self._date_string(merged.get("fecha_objetivo"))
        merged["updated_by"] = clean_text(actor)
        merged["updated_at"] = utc_now()
        merged["version"] = current_version + 1
        changed = {
            key: (old.get(key), merged.get(key))
            for key in (
                "ficha",
                "nombre_ficha",
                "producto",
                "proveedor",
                "marca",
                "descripcion",
                "estado",
                "responsable",
                "prioridad",
                "fecha_objetivo",
            )
            if str(old.get(key, "")) != str(merged.get(key, ""))
        }
        if not changed:
            return old
        update_columns = [
            "identity_key",
            "ficha",
            "nombre_ficha",
            "producto",
            "proveedor",
            "proveedor_norm",
            "marca",
            "marca_norm",
            "descripcion",
            "estado",
            "responsable",
            "prioridad",
            "fecha_objetivo",
            "updated_by",
            "updated_at",
            "version",
        ]
        assignments = ",".join(f"{column}=:{column}" for column in update_columns)
        try:
            with self.engine.begin() as connection:
                result = connection.execute(
                    text(
                        f"UPDATE pipeline_cards SET {assignments} "
                        "WHERE id=:id AND version=:expected_version"
                    ),
                    {**merged, "id": card_id, "expected_version": current_version},
                )
                if result.rowcount != 1:
                    raise PipelineError(
                        "La tarjeta fue modificada por otro usuario. Recarga la página antes de guardar."
                    )
                snapshot = _row_dict(
                    connection.execute(
                        text("SELECT * FROM pipeline_cards WHERE id=:id"), {"id": card_id}
                    ).first()
                )
                self._queue_outbox(connection, "card", card_id, "upsert", snapshot)
                for field, (before, after) in changed.items():
                    self._audit(
                        connection,
                        card_id=card_id,
                        action="card_updated",
                        actor=actor,
                        field_name=field,
                        old_value=before,
                        new_value=after,
                    )
        except Exception as exc:
            if "unique" in str(exc).lower():
                raise PipelineError(
                    "El cambio produciria una tarjeta duplicada para ficha, proveedor y marca."
                ) from exc
            raise
        return self.get_card(card_id)

    def duplicate_card(self, card_id: str, *, actor: str) -> dict[str, Any]:
        """Duplica una tarjeta completa sin duplicar fisicamente archivos de Drive.

        Se copian datos, avance, contactos y referencias documentales. Cada fila
        nueva recibe su propio id para mantener auditoria y sincronizacion
        independientes. Los documentos apuntan al mismo archivo seguro en Drive.
        """
        original = self.get_card(card_id)
        if _as_bool(original.get("archived")):
            raise PipelineError("No se puede duplicar una tarjeta eliminada.")

        now = utc_now()
        duplicate_id = uuid.uuid4().hex
        canonical_identity = identity_key(
            ficha=original.get("ficha"),
            producto=original.get("producto"),
            proveedor=original.get("proveedor"),
            marca=original.get("marca"),
        )
        duplicate = {
            column: original.get(column, "")
            for column in CARD_COLUMNS
        }
        duplicate.update(
            {
                "id": duplicate_id,
                "identity_key": f"{canonical_identity}{COPY_IDENTITY_MARKER}{duplicate_id}",
                "archived": 0,
                "source": "duplicate",
                "source_external_id": "",
                "created_by": clean_text(actor),
                "updated_by": clean_text(actor),
                "created_at": now,
                "updated_at": now,
                "version": 1,
            }
        )

        with self.engine.begin() as connection:
            original_checkpoints = [
                _row_dict(row)
                for row in connection.execute(
                    text(
                        """SELECT * FROM pipeline_checkpoints
                        WHERE card_id=:card ORDER BY position"""
                    ),
                    {"card": card_id},
                )
            ]
            original_contacts = [
                _row_dict(row)
                for row in connection.execute(
                    text(
                        """SELECT * FROM pipeline_contacts
                        WHERE card_id=:card AND archived=0 ORDER BY created_at"""
                    ),
                    {"card": card_id},
                )
            ]
            original_documents = [
                _row_dict(row)
                for row in connection.execute(
                    text(
                        """SELECT * FROM pipeline_documents
                        WHERE card_id=:card AND archived=0 ORDER BY uploaded_at"""
                    ),
                    {"card": card_id},
                )
            ]

            columns = ",".join(CARD_COLUMNS)
            values = ",".join(f":{column}" for column in CARD_COLUMNS)
            connection.execute(
                text(f"INSERT INTO pipeline_cards ({columns}) VALUES ({values})"),
                duplicate,
            )

            for source_checkpoint in original_checkpoints:
                checkpoint = {
                    column: source_checkpoint.get(column, "")
                    for column in CHECKPOINT_COLUMNS
                }
                checkpoint.update(
                    {
                        "id": uuid.uuid4().hex,
                        "card_id": duplicate_id,
                        "updated_at": now,
                    }
                )
                cp_columns = ",".join(CHECKPOINT_COLUMNS)
                cp_values = ",".join(f":{column}" for column in CHECKPOINT_COLUMNS)
                connection.execute(
                    text(
                        f"INSERT INTO pipeline_checkpoints ({cp_columns}) VALUES ({cp_values})"
                    ),
                    checkpoint,
                )
                self._queue_outbox(
                    connection, "checkpoint", checkpoint["id"], "upsert", checkpoint
                )

            for source_contact in original_contacts:
                contact = {
                    column: source_contact.get(column, "")
                    for column in CONTACT_COLUMNS
                }
                contact.update(
                    {
                        "id": uuid.uuid4().hex,
                        "card_id": duplicate_id,
                        "archived": 0,
                        "created_by": clean_text(actor),
                        "created_at": now,
                        "updated_at": now,
                    }
                )
                contact_columns = ",".join(CONTACT_COLUMNS)
                contact_values = ",".join(
                    f":{column}" for column in CONTACT_COLUMNS
                )
                connection.execute(
                    text(
                        f"INSERT INTO pipeline_contacts ({contact_columns}) "
                        f"VALUES ({contact_values})"
                    ),
                    contact,
                )
                self._queue_outbox(
                    connection, "contact", contact["id"], "upsert", contact
                )

            for source_document in original_documents:
                document = {
                    column: source_document.get(column, "")
                    for column in DOCUMENT_COLUMNS
                }
                document.update(
                    {
                        "id": uuid.uuid4().hex,
                        "card_id": duplicate_id,
                        "archived": 0,
                        "uploaded_by": clean_text(actor),
                        "uploaded_at": now,
                    }
                )
                document_columns = ",".join(DOCUMENT_COLUMNS)
                document_values = ",".join(
                    f":{column}" for column in DOCUMENT_COLUMNS
                )
                connection.execute(
                    text(
                        f"INSERT INTO pipeline_documents ({document_columns}) "
                        f"VALUES ({document_values})"
                    ),
                    document,
                )
                self._queue_outbox(
                    connection, "document", document["id"], "upsert", document
                )

            self._queue_outbox(
                connection, "card", duplicate_id, "upsert", duplicate
            )
            self._audit(
                connection,
                card_id=duplicate_id,
                action="card_duplicated_from",
                actor=actor,
                new_value={"source_card_id": card_id},
            )
            self._audit(
                connection,
                card_id=card_id,
                action="card_duplicated_to",
                actor=actor,
                new_value={"duplicate_card_id": duplicate_id},
            )
        return self.get_card(duplicate_id)

    def change_route(
        self,
        card_id: str,
        *,
        route_key: str,
        actor: str,
        confirm_reset: bool = False,
    ) -> dict[str, Any]:
        if route_key not in ROUTES:
            raise PipelineError("La categoría seleccionada no existe.")
        card = self.get_card(card_id)
        if card["route_key"] == route_key:
            return card
        checkpoints = self.checkpoints(card_id)
        if any(_as_bool(row.get("completed")) for row in checkpoints) and not confirm_reset:
            raise PipelineRuleError(
                "Cambiar la categoría reiniciará la lista de comprobación.",
                requires_confirmation=True,
            )
        now = utc_now()
        with self.engine.begin() as connection:
            for row in checkpoints:
                connection.execute(
                    text("DELETE FROM pipeline_checkpoints WHERE id=:id"), {"id": row["id"]}
                )
                deleted = dict(row)
                deleted["archived"] = 1
                self._queue_outbox(connection, "checkpoint", row["id"], "archive", deleted)
            for position, (key, label) in enumerate(ROUTES[route_key].checklist, start=1):
                checkpoint = {
                    "id": uuid.uuid4().hex,
                    "card_id": card_id,
                    "checkpoint_key": key,
                    "position": position,
                    "label": label,
                    "completed": 0,
                    "completed_at": "",
                    "completed_by": "",
                    "notes": "",
                    "updated_at": now,
                }
                columns = ",".join(CHECKPOINT_COLUMNS)
                values = ",".join(f":{column}" for column in CHECKPOINT_COLUMNS)
                connection.execute(
                    text(f"INSERT INTO pipeline_checkpoints ({columns}) VALUES ({values})"),
                    checkpoint,
                )
                self._queue_outbox(connection, "checkpoint", checkpoint["id"], "upsert", checkpoint)
            connection.execute(
                text(
                    """UPDATE pipeline_cards SET route_key=:route,updated_by=:actor,
                    updated_at=:now,version=version+1 WHERE id=:id"""
                ),
                {"route": route_key, "actor": clean_text(actor), "now": now, "id": card_id},
            )
            updated = _row_dict(
                connection.execute(
                    text("SELECT * FROM pipeline_cards WHERE id=:id"), {"id": card_id}
                ).first()
            )
            self._queue_outbox(connection, "card", card_id, "upsert", updated)
            self._audit(
                connection,
                card_id=card_id,
                action="route_changed",
                actor=actor,
                field_name="route_key",
                old_value=card["route_key"],
                new_value=route_key,
            )
        return self.get_card(card_id)

    def archive_card(
        self,
        card_id: str,
        *,
        actor: str,
        archived: bool = True,
        expected_version: int | None = None,
    ) -> None:
        card = self.get_card(card_id)
        current_version = int(card.get("version") or 1)
        if expected_version is not None and int(expected_version) != current_version:
            raise PipelineError(
                "La tarjeta fue modificada por otro usuario. Recarga la pagina antes de eliminarla."
            )
        now = utc_now()
        with self.engine.begin() as connection:
            result = connection.execute(
                text(
                    """UPDATE pipeline_cards SET archived=:archived,updated_by=:actor,
                    updated_at=:now,version=version+1 WHERE id=:id AND version=:expected_version"""
                ),
                {
                    "archived": 1 if archived else 0,
                    "actor": clean_text(actor),
                    "now": now,
                    "id": card_id,
                    "expected_version": current_version,
                },
            )
            if result.rowcount != 1:
                raise PipelineError(
                    "La tarjeta fue modificada por otro usuario. Recarga la pagina antes de eliminarla."
                )
            snapshot = _row_dict(
                connection.execute(
                    text("SELECT * FROM pipeline_cards WHERE id=:id"), {"id": card_id}
                ).first()
            )
            self._queue_outbox(connection, "card", card_id, "upsert", snapshot)
            self._audit(
                connection,
                card_id=card_id,
                action="card_archived" if archived else "card_restored",
                actor=actor,
                old_value=_as_bool(card.get("archived")),
                new_value=archived,
            )

    def add_contact(
        self,
        card_id: str,
        *,
        actor: str,
        nombre: str = "",
        cargo: str = "",
        email: str = "",
        telefono: str = "",
        whatsapp_wechat: str = "",
        pais: str = "",
        canal_preferido: str = "",
        notas: str = "",
        es_principal: bool = False,
    ) -> dict[str, Any]:
        self.get_card(card_id)
        if not any(clean_text(value) for value in (nombre, email, telefono, whatsapp_wechat)):
            raise PipelineError("Indica al menos nombre, correo, telefono o WhatsApp/WeChat.")
        now = utc_now()
        row = {
            "id": uuid.uuid4().hex,
            "card_id": card_id,
            "nombre": clean_text(nombre),
            "cargo": clean_text(cargo),
            "email": clean_text(email),
            "telefono": clean_text(telefono),
            "whatsapp_wechat": clean_text(whatsapp_wechat),
            "pais": clean_text(pais),
            "canal_preferido": clean_text(canal_preferido),
            "notas": clean_text(notas),
            "es_principal": 1 if es_principal else 0,
            "archived": 0,
            "created_by": clean_text(actor),
            "created_at": now,
            "updated_at": now,
        }
        with self.engine.begin() as connection:
            if es_principal:
                previous_primary = [
                    _row_dict(existing)
                    for existing in connection.execute(
                        text(
                            """SELECT * FROM pipeline_contacts
                            WHERE card_id=:card AND archived=0 AND es_principal=1"""
                        ),
                        {"card": card_id},
                    )
                ]
                connection.execute(
                    text(
                        "UPDATE pipeline_contacts SET es_principal=0,updated_at=:now WHERE card_id=:card"
                    ),
                    {"now": now, "card": card_id},
                )
                for previous in previous_primary:
                    previous["es_principal"] = 0
                    previous["updated_at"] = now
                    self._queue_outbox(
                        connection,
                        "contact",
                        previous["id"],
                        "upsert",
                        previous,
                    )
            columns = ",".join(CONTACT_COLUMNS)
            values = ",".join(f":{column}" for column in CONTACT_COLUMNS)
            connection.execute(
                text(f"INSERT INTO pipeline_contacts ({columns}) VALUES ({values})"), row
            )
            self._queue_outbox(connection, "contact", row["id"], "upsert", row)
            self._audit(
                connection,
                card_id=card_id,
                action="contact_added",
                actor=actor,
                new_value={"id": row["id"], "nombre": row["nombre"]},
            )
        return row

    def contacts(self, card_id: str) -> list[dict[str, Any]]:
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """SELECT * FROM pipeline_contacts WHERE card_id=:card AND archived=0
                    ORDER BY es_principal DESC, created_at"""
                ),
                {"card": card_id},
            )
            return [_row_dict(row) for row in rows]

    def archive_contact(self, contact_id: str, *, actor: str) -> None:
        with self.engine.begin() as connection:
            row = _row_dict(
                connection.execute(
                    text("SELECT * FROM pipeline_contacts WHERE id=:id"), {"id": contact_id}
                ).first()
            )
            if not row:
                return
            row["archived"] = 1
            row["updated_at"] = utc_now()
            connection.execute(
                text(
                    "UPDATE pipeline_contacts SET archived=1,updated_at=:updated_at WHERE id=:id"
                ),
                row,
            )
            self._queue_outbox(connection, "contact", contact_id, "upsert", row)
            self._audit(
                connection,
                card_id=row["card_id"],
                action="contact_archived",
                actor=actor,
                old_value={"id": contact_id, "nombre": row.get("nombre", "")},
            )

    def add_document(
        self,
        card_id: str,
        *,
        actor: str,
        file_name: str,
        file_url: str,
        drive_file_id: str = "",
        document_type: str = "",
        mime_type: str = "",
        size_bytes: int = 0,
        descripcion: str = "",
        storage_provider: str = "drive",
    ) -> dict[str, Any]:
        self.get_card(card_id)
        now = utc_now()
        row = {
            "id": uuid.uuid4().hex,
            "card_id": card_id,
            "storage_provider": clean_text(storage_provider) or "drive",
            "drive_file_id": clean_text(drive_file_id),
            "file_url": clean_text(file_url),
            "file_name": clean_text(file_name) or "Documento",
            "document_type": clean_text(document_type),
            "mime_type": clean_text(mime_type),
            "size_bytes": max(0, int(size_bytes or 0)),
            "descripcion": clean_text(descripcion),
            "archived": 0,
            "uploaded_by": clean_text(actor),
            "uploaded_at": now,
        }
        with self.engine.begin() as connection:
            columns = ",".join(DOCUMENT_COLUMNS)
            values = ",".join(f":{column}" for column in DOCUMENT_COLUMNS)
            connection.execute(
                text(f"INSERT INTO pipeline_documents ({columns}) VALUES ({values})"), row
            )
            self._queue_outbox(connection, "document", row["id"], "upsert", row)
            self._audit(
                connection,
                card_id=card_id,
                action="document_added",
                actor=actor,
                new_value={"id": row["id"], "file_name": row["file_name"]},
            )
        return row

    def documents(self, card_id: str) -> list[dict[str, Any]]:
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """SELECT * FROM pipeline_documents WHERE card_id=:card AND archived=0
                    ORDER BY uploaded_at DESC"""
                ),
                {"card": card_id},
            )
            return [_row_dict(row) for row in rows]

    def archive_document(self, document_id: str, *, actor: str) -> None:
        with self.engine.begin() as connection:
            row = _row_dict(
                connection.execute(
                    text("SELECT * FROM pipeline_documents WHERE id=:id"),
                    {"id": document_id},
                ).first()
            )
            if not row:
                return
            row["archived"] = 1
            connection.execute(
                text("UPDATE pipeline_documents SET archived=1 WHERE id=:id"),
                {"id": document_id},
            )
            self._queue_outbox(connection, "document", document_id, "upsert", row)
            self._audit(
                connection,
                card_id=row["card_id"],
                action="document_archived",
                actor=actor,
                old_value={"id": document_id, "file_name": row.get("file_name", "")},
            )

    def activities(self, card_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """SELECT * FROM pipeline_activity WHERE card_id=:card
                    ORDER BY created_at DESC LIMIT :limit"""
                ),
                {"card": card_id, "limit": max(1, min(500, int(limit)))},
            )
            return [_row_dict(row) for row in rows]

    def analytics(self, filters: PipelineFilters | None = None) -> dict[str, Any]:
        cards = self.list_cards(filters)
        if not cards:
            return {
                "total_cards": 0,
                "average_progress": 0.0,
                "ready_to_bid": 0,
                "completed": 0,
                "routes": [],
                "funnel": [],
            }
        card_ids = [card["id"] for card in cards]
        checkpoints_by_card = self._checkpoints_for_cards(card_ids)
        ready = 0
        completed = 0
        route_buckets: dict[str, list[float]] = {key: [] for key in ROUTES}
        funnel_buckets: dict[tuple[str, str, int, str], int] = {}
        for card in cards:
            progress = float(card.get("progress") or 0.0)
            route_buckets.setdefault(card["route_key"], []).append(progress)
            checkpoints = checkpoints_by_card.get(card["id"], [])
            if any(
                row["checkpoint_key"] == "ct_aprobado" and _as_bool(row["completed"])
                for row in checkpoints
            ):
                ready += 1
            if checkpoints and all(_as_bool(row["completed"]) for row in checkpoints):
                completed += 1
            for checkpoint in checkpoints:
                key = (
                    card["route_key"],
                    checkpoint["checkpoint_key"],
                    int(checkpoint["position"]),
                    checkpoint["label"],
                )
                funnel_buckets[key] = funnel_buckets.get(key, 0) + (
                    1 if _as_bool(checkpoint["completed"]) else 0
                )
        routes = []
        for route_key, values in route_buckets.items():
            if not values:
                continue
            routes.append(
                {
                    "route_key": route_key,
                    "route": ROUTES[route_key].short_label,
                    "tarjetas": len(values),
                    "avance_promedio": round(sum(values) / len(values), 1),
                }
            )
        funnel = [
            {
                "route_key": route_key,
                "route": ROUTES[route_key].short_label,
                "checkpoint_key": checkpoint_key,
                "position": position,
                "control": label,
                "completadas": count,
                "total_tarjetas": len(route_buckets.get(route_key, [])),
                "porcentaje": round(
                    count / len(route_buckets.get(route_key, [])) * 100.0, 1
                )
                if route_buckets.get(route_key)
                else 0.0,
            }
            for (route_key, checkpoint_key, position, label), count in funnel_buckets.items()
        ]
        funnel.sort(key=lambda row: (list(ROUTES).index(row["route_key"]), row["position"]))
        return {
            "total_cards": len(cards),
            "average_progress": round(
                sum(float(card.get("progress") or 0.0) for card in cards) / len(cards), 1
            ),
            "ready_to_bid": ready,
            "completed": completed,
            "routes": routes,
            "funnel": funnel,
        }

    def _checkpoints_for_cards(
        self, card_ids: Sequence[str]
    ) -> dict[str, list[dict[str, Any]]]:
        if not card_ids:
            return {}
        params: dict[str, str] = {}
        placeholders: list[str] = []
        for index, card_id in enumerate(card_ids):
            key = f"card_{index}"
            params[key] = card_id
            placeholders.append(f":{key}")
        with self.engine.connect() as connection:
            rows = [
                _row_dict(row)
                for row in connection.execute(
                    text(
                        f"""SELECT * FROM pipeline_checkpoints
                        WHERE card_id IN ({','.join(placeholders)}) ORDER BY card_id,position"""
                    ),
                    params,
                )
            ]
        output: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            output.setdefault(row["card_id"], []).append(row)
        return output

    def list_outbox(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """SELECT * FROM pipeline_sync_outbox
                    WHERE status IN ('pending','error') ORDER BY created_at LIMIT :limit"""
                ),
                {"limit": max(1, min(1000, int(limit)))},
            )
            output = []
            for row in rows:
                item = _row_dict(row)
                try:
                    item["payload_data"] = json.loads(item.get("payload") or "{}")
                except json.JSONDecodeError:
                    item["payload_data"] = {}
                output.append(item)
            return output

    def mark_outbox_synced(self, outbox_ids: Iterable[str]) -> None:
        ids = [clean_text(value) for value in outbox_ids if clean_text(value)]
        if not ids:
            return
        now = utc_now()
        with self.engine.begin() as connection:
            for item_id in ids:
                connection.execute(
                    text(
                        """UPDATE pipeline_sync_outbox SET status='synced',attempts=attempts+1,
                        last_error='',updated_at=:now,synced_at=:now WHERE id=:id"""
                    ),
                    {"now": now, "id": item_id},
                )

    def mark_outbox_error(self, outbox_id: str, error: Any) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    """UPDATE pipeline_sync_outbox SET status='error',attempts=attempts+1,
                    last_error=:error,updated_at=:now WHERE id=:id"""
                ),
                {
                    "error": clean_text(error)[:1500],
                    "now": utc_now(),
                    "id": outbox_id,
                },
            )

    def outbox_counts(self) -> dict[str, int]:
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    "SELECT status,COUNT(*) AS total FROM pipeline_sync_outbox GROUP BY status"
                )
            )
            counts = {str(row.status): int(row.total) for row in rows}
        return {
            "pending": counts.get("pending", 0),
            "error": counts.get("error", 0),
            "synced": counts.get("synced", 0),
        }

    def apply_imported_checkpoints(
        self,
        card_id: str,
        completed_keys: Sequence[str],
        *,
        actor: str,
    ) -> None:
        """Aplica solo el prefijo consecutivo completado de una importacion."""
        completed = set(completed_keys)
        for checkpoint in self.checkpoints(card_id):
            if checkpoint["checkpoint_key"] not in completed:
                break
            self.set_checkpoint(
                card_id=card_id,
                checkpoint_key=checkpoint["checkpoint_key"],
                completed=True,
                actor=actor,
            )


__all__ = [
    "ACTIVITY_COLUMNS",
    "CARD_COLUMNS",
    "CHECKPOINT_COLUMNS",
    "CONTACT_COLUMNS",
    "DOCUMENT_COLUMNS",
    "MIRROR_COLUMNS",
    "PipelineError",
    "PipelineFilters",
    "PipelineRepository",
    "PipelineRuleError",
    "ROUTES",
    "RouteDefinition",
    "STANDARD_CHECKLIST",
    "clean_text",
    "identity_key",
    "normalize_ficha",
    "normalize_key",
]
