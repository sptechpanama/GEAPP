from services.ct_rir_registry import (
    merge_registry_tokens,
    parse_registry_values,
    registry_sheet_values,
)


def test_legacy_registry_does_not_treat_username_as_ficha_name() -> None:
    records = parse_registry_values(
        [
            ["Ficha #", "Actualizado por", "Actualizado"],
            ["43358", "rsanchez", "2026-08-10 09:00:00"],
        ]
    )
    assert records == [
        {
            "ficha": "43358",
            "nombre": "",
            "actualizado_por": "rsanchez",
            "actualizado": "2026-08-10 09:00:00",
        }
    ]


def test_registry_persists_number_and_catalog_name() -> None:
    records = merge_registry_tokens(
        [],
        ["43358", "100523"],
        name_lookup={
            "43358": "KIT DE CIRCUITO DE PACIENTE",
            "100523": "TUBO NASOGASTRICO",
        },
    )
    values = registry_sheet_values(
        records,
        updated_by="rsanchez",
        updated_at="2026-08-10 09:00:00",
    )
    restored = parse_registry_values(values)
    assert [record["ficha"] for record in restored] == ["43358", "100523"]
    assert [record["nombre"] for record in restored] == [
        "KIT DE CIRCUITO DE PACIENTE",
        "TUBO NASOGASTRICO",
    ]


def test_registry_removes_only_requested_ficha() -> None:
    records = merge_registry_tokens([], ["43358", "100523", "103496"])
    remaining = merge_registry_tokens(records, ["100523"], remove=True)
    assert [record["ficha"] for record in remaining] == ["43358", "103496"]
