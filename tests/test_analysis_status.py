import os
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

os.environ.setdefault("DATABASE_URL", "postgresql://user:password@localhost:5432/db")
os.environ.setdefault("RABBITMQ_USER", "worker")
os.environ.setdefault("RABBITMQ_PASSWORD", "secret")

import main


def img(status=None, avisos=None):
    result = {}
    if status is not None:
        result["status_analise"] = status
    if avisos is not None:
        result["avisos"] = avisos
    return result


class AggregateStatusTests(unittest.TestCase):
    def test_old_identifier_without_status_returns_none(self):
        self.assertIsNone(main.aggregate_status([img(), img()], 0))
        self.assertIsNone(main.aggregate_status([], 0))

    def test_ignores_non_string_status(self):
        self.assertIsNone(main.aggregate_status([{"status_analise": 1}], 0))

    def test_all_ok_with_guaras(self):
        self.assertEqual(main.aggregate_status([img("ok"), img("ok")], 3), "ok")

    def test_any_atencao_with_guaras(self):
        self.assertEqual(main.aggregate_status([img("ok"), img("atencao")], 2), "atencao")

    def test_no_guaras_prefers_sem_guara(self):
        self.assertEqual(main.aggregate_status([img("sem_ave"), img("sem_guara")], 0), "sem_guara")

    def test_no_guaras_all_sem_ave(self):
        self.assertEqual(main.aggregate_status([img("sem_ave"), img("sem_ave")], 0), "sem_ave")

    def test_guara_in_one_image_overrides_sem_ave_in_other(self):
        self.assertEqual(main.aggregate_status([img("sem_ave"), img("ok")], 1), "ok")


class AggregateAvisosTests(unittest.TestCase):
    def test_none_when_no_image_sends_field(self):
        self.assertIsNone(main.aggregate_avisos([img(), img()]))
        self.assertIsNone(main.aggregate_avisos([]))

    def test_empty_list_when_field_present_but_empty(self):
        self.assertEqual(main.aggregate_avisos([img(avisos=[])]), [])

    def test_dedupes_preserving_order(self):
        results = [
            img(avisos=["nenhuma_ave_detectada"]),
            img(avisos=["nenhum_guara_identificado", "nenhuma_ave_detectada"]),
        ]
        self.assertEqual(
            main.aggregate_avisos(results),
            ["nenhuma_ave_detectada", "nenhum_guara_identificado"],
        )

    def test_skips_non_string_items_and_non_list_values(self):
        results = [img(avisos=["a", 1, None]), {"avisos": "x"}]
        self.assertEqual(main.aggregate_avisos(results), ["a"])


if __name__ == "__main__":
    unittest.main()
