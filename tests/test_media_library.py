import importlib.util
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

spec = importlib.util.spec_from_file_location(
    "media_library", Path(__file__).resolve().parents[1] / "Backend/helper/media_library.py",
)
library = importlib.util.module_from_spec(spec)
spec.loader.exec_module(library)


class Cursor:
    def __init__(self, docs):
        self.docs = iter(docs)
        self.closed = False

    def sort(self, fields):
        return self

    def batch_size(self, size):
        return self

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.docs)
        except StopIteration:
            raise StopAsyncIteration

    async def close(self):
        self.closed = True


class Collection:
    def __init__(self, docs):
        self.docs = docs
        self.queries = []

    async def count_documents(self, query):
        return len(self.docs)

    def find(self, query, projection=None):
        self.queries.append(query)
        self.cursor = Cursor(self.docs)
        return self.cursor


class LibraryTests(unittest.IsolatedAsyncioTestCase):
    async def test_combined_global_order_pages_types_and_catalog_tags(self):
        def doc(id, day):
            return {"_id": str(day), "tmdb_id": id, "title": str(id), "updated_on": datetime(2026, 1, day)}
        movies = Collection([doc(10, 4), doc(-20, 2)])
        shows = Collection([doc(10, 3)])
        older = Collection([doc(30, 1)])
        catalogs = Collection([{"_id": "a" * 24, "name": "Untagged", "items": [
            {"media_type": "movie", "tmdb_id": -20, "db_index": 1},
        ]}])
        db = SimpleNamespace(dbs={"tracking": {"custom_catalogs": catalogs},
                                 "storage_1": {"movie": movies, "tv": shows},
                                 "storage_3": {"movie": older, "tv": Collection([])}})
        first = await library.list_library(db, page_size=2)
        second = await library.list_library(db, page=2, page_size=2)
        self.assertEqual([x["media_type"] for x in first["items"]], ["movie", "tv"])
        self.assertEqual([x["tmdb_id"] for x in second["items"]], [-20, 30])
        self.assertEqual(second["total_count"], 4)
        self.assertEqual(second["total_pages"], 2)
        self.assertEqual(second["databases_checked"], [1, 3])
        self.assertEqual(second["items"][0]["catalog_tags"][0]["name"], "Untagged")
        self.assertTrue(movies.cursor.closed)
        self.assertNotIn("_id", first["items"][0])
        clamped = await library.list_library(db, page=99, page_size=2)
        self.assertEqual(clamped["current_page"], 2)

    async def test_filtered_queries_and_empty_library(self):
        movies = Collection([])
        db = SimpleNamespace(dbs={"storage_1": {"movie": movies}})
        result = await library.list_library(db, "movie", search="A.B", custom=True)
        self.assertEqual(result["total_count"], 0)
        self.assertEqual(result["movies"], [])
        query = movies.queries[0]
        self.assertEqual(query["tmdb_id"], {"$lt": 0})
        self.assertEqual(query["$or"][0]["title"]["$regex"], r"A\.B")

    def test_validation_and_deduplication(self):
        ref = {"media_type": "movie", "tmdb_id": -2, "db_index": 1}
        payload = {"action": "delete", "confirm_delete": True, "items": [ref, ref]}
        self.assertEqual(len(library.normalize_batch(payload)[1]), 1)
        for change in ({"confirm_delete": False}, {"items": []}, {"items": [ref] * 101},
                       {"items": [{**ref, "media_type": "all"}]},
                       {"items": [{**ref, "tmdb_id": True}]},
                       {"items": [{**ref, "tmdb_id": 1.5}]}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                library.normalize_batch({**payload, **change})

    async def test_bulk_partial_failure_and_exact_typed_refs(self):
        refs = [{"media_type": kind, "tmdb_id": 10, "db_index": 1} for kind in ("movie", "tv")]
        db = SimpleNamespace(dbs={"storage_1": {}}, get_document=AsyncMock(return_value={"title": "Media"}))
        delete = AsyncMock(side_effect=[{}, RuntimeError("failed")])
        result = await library.bulk_library(db, {"action": "delete", "items": refs, "confirm_delete": True}, delete, AsyncMock())
        self.assertEqual(result["succeeded"], 1)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["results"][1]["media_type"], "tv")
        self.assertEqual(delete.call_args_list[0].args, (10, 1, "movie"))
        self.assertEqual(delete.call_args_list[1].args, (10, 1, "tv"))

    async def test_bad_catalog_prevents_all_mutations(self):
        db = SimpleNamespace(get_custom_catalog=AsyncMock(return_value=None))
        add = AsyncMock()
        with self.assertRaises(ValueError):
            await library.bulk_library(db, {"action": "add_to_catalogs", "catalog_ids": ["a" * 24],
                "items": [{"media_type": "movie", "tmdb_id": 10, "db_index": 1}]}, AsyncMock(), add)
        add.assert_not_awaited()

    async def test_catalog_partial_failure_is_reported(self):
        db = SimpleNamespace(dbs={"storage_1": {}}, get_document=AsyncMock(return_value={"title": "Media"}),
                             get_custom_catalog=AsyncMock(return_value={"name": "Catalogue"}))
        add = AsyncMock(side_effect=[{}, RuntimeError("write failed")])
        result = await library.bulk_library(db, {"action": "add_to_catalogs", "catalog_ids": ["a" * 24, "b" * 24],
            "items": [{"media_type": "movie", "tmdb_id": 10, "db_index": 1}]}, AsyncMock(), add)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["results"][0]["catalog_ids"], ["a" * 24])

    def test_no_promotional_stream_injection(self):
        source = (Path(__file__).resolve().parents[1] / "Backend/fastapi/routes/stremio_routes.py").read_text()
        self.assertNotIn("_donation", source)
        self.assertNotIn("donate.weebzonex.workers.dev", source)


if __name__ == "__main__":
    unittest.main()
