from __future__ import annotations

import sys
import unittest
from pathlib import Path


APP_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP_ROOT))

from services.ct_rir_detection import detect_watched_fichas  # noqa: E402


class CtRirDetectionTests(unittest.TestCase):
    def test_recovers_real_cl_048336_with_plural_variation(self) -> None:
        fields = {
            "titulo": (
                "KIT DE CIRCUITO DE PACIENTES PARA MAQUINA DE ANESTESIA, "
                "SE UTILIZA PARA ADMINISTRAR GASES ANESTESICOS"
            ),
            "item_1": "Circuito para pacientes adulto con accesorios",
        }
        watched = {
            "43358": "KIT DE CIRCUITO DE PACIENTE PARA MAQUINA DE ANESTESIA",
            "103169": "JUEGO DE CICLO COMPLETO DE ESTERILIZACION",
        }
        self.assertEqual(detect_watched_fichas(fields, watched), ("43358",))

    def test_does_not_match_generic_anesthesia_text(self) -> None:
        watched = {
            "43358": "KIT DE CIRCUITO DE PACIENTE PARA MAQUINA DE ANESTESIA",
        }
        for text in (
            "Compra de kits para pacientes de anestesia",
            "Gases anestesicos y circuitos diversos",
            "Repuestos para maquina hospitalaria",
        ):
            with self.subTest(text=text):
                self.assertEqual(
                    detect_watched_fichas({"descripcion": text}, watched),
                    (),
                )


if __name__ == "__main__":
    unittest.main()

