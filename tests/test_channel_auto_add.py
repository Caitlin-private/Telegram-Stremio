import importlib.util
from pathlib import Path
import unittest
from unittest.mock import AsyncMock

# Load pure capture logic without starting Telegram clients or connecting databases.
spec = importlib.util.spec_from_file_location(
    "channel_auto_add", Path(__file__).resolve().parents[1] / "Backend/helper/channel_auto_add.py",
)
capture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capture)


class CaptureTests(unittest.TestCase):
    def test_five_minute_window_and_delay(self):
        session = capture.build_session({"duration_minutes": 5, "delay_minutes": 5}, now=1000)
        self.assertEqual(capture.session_status(session, 1299), "scheduled")
        self.assertEqual(capture.session_status(session, 1300), "active")
        self.assertTrue(capture.accepts_message(session, 1300, 1300))
        self.assertFalse(capture.accepts_message(session, 1299, 1300))
        self.assertFalse(capture.accepts_message(session, 1600, 1600))
        self.assertEqual(capture.session_status(session, 1600), "expired")

    def test_indefinite_and_stop(self):
        session = capture.build_session({"duration_minutes": None}, now=1000)
        self.assertTrue(capture.accepts_message(session, 999999, 999999))
        session["enabled"] = False
        self.assertFalse(capture.accepts_message(session, 999999, 999999))
        self.assertEqual(capture.session_status({}), "off")

    def test_blank_fields_use_exact_filename_without_fake_metadata(self):
        session = capture.build_session({"manual_metadata": {"title": "  "}})
        meta = capture.capture_metadata(session, "My.Raw.File.2026.mkv", {}, 123, 5)
        self.assertTrue(session["untagged"])
        self.assertEqual(meta["title"], "My.Raw.File.2026.mkv")
        self.assertEqual(meta["genres"], [])
        self.assertEqual(meta["rate"], 0)
        self.assertEqual(meta["description"], "")
        self.assertLess(meta["tmdb_id"], 0)
        self.assertTrue(meta["imdb_id"].startswith("tgauto"))

    def test_custom_fields_and_episode_zero(self):
        session = capture.build_session({
            "media_type": "tv", "season_number": 0, "episode_number": 0,
            "manual_metadata": {"title": "Custom", "rate": "0", "year": "2026", "genres": "Drama, Comedy"},
            "quality": "4K HDR",
        })
        meta = capture.capture_metadata(session, "file.mkv", {"season": 2, "episode": 3}, 123, 5)
        self.assertFalse(session["untagged"])
        self.assertEqual(meta["title"], "Custom")
        self.assertEqual(meta["genres"], ["Drama", "Comedy"])
        self.assertEqual(meta["quality"], "4K HDR")
        self.assertEqual(meta["season_number"], 0)
        self.assertEqual(meta["episode_number"], 0)
        self.assertEqual(meta["rate"], 0)

    def test_catalog_selection_is_not_untagged(self):
        session = capture.build_session({"catalog_ids": ["chosen", "chosen"]})
        self.assertFalse(session["untagged"])
        self.assertEqual(session["catalog_ids"], ["chosen"])

    def test_stable_identity_and_split_grouping(self):
        session = capture.build_session({})
        first = capture.capture_identity(session, 123, 5)
        self.assertEqual(first, capture.capture_identity(session, 123, 5))
        self.assertNotEqual(first, capture.capture_identity(session, 123, 6))
        self.assertNotEqual(first, capture.capture_identity(session, 456, 5))
        self.assertEqual(capture.capture_identity(session, 123, 5, "video.mkv"),
                         capture.capture_identity(session, 123, 6, "video.mkv"))
        self.assertNotEqual(capture.capture_identity(session, 123, 5, "video.mkv"),
                            capture.capture_identity(capture.build_session({}), 123, 5, "video.mkv"))

    def test_invalid_parameters(self):
        for payload in (
            {"duration_minutes": 0}, {"duration_minutes": -1}, {"duration_minutes": True},
            {"delay_minutes": 1.5}, {"delay_minutes": 1441}, {"media_type": "other"},
            {"manual_metadata": {"rate": "NaN"}}, {"manual_metadata": {"rate": "11"}},
            {"manual_metadata": {"year": "no"}}, {"manual_metadata": {"title": []}},
            {"catalog_ids": "id"}, {"season_number": -1},
        ):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                capture.build_session(payload)


class SessionPersistenceTests(unittest.IsolatedAsyncioTestCase):
    def make_db(self):
        class DB:
            pass
        db = DB()
        db.dbs = {"tracking": {"state": AsyncMock()}}
        db.get_custom_catalog = AsyncMock()
        return db

    async def test_start_and_stop_are_persisted(self):
        db = self.make_db()
        session = await capture.start_session(db, {"duration_minutes": None})
        state = db.dbs["tracking"]["state"]
        stored = state.replace_one.call_args.args[1]
        self.assertTrue(stored["enabled"])
        self.assertIsNone(stored["expires_at"])
        self.assertEqual(session["status"], "active")
        state.find_one.return_value = dict(stored)
        self.assertEqual((await capture.get_session(db))["session_id"], session["session_id"])
        self.assertNotIn("_id", await capture.get_session(db))
        self.assertEqual((await capture.stop_session(db))["status"], "off")
        state.update_one.assert_awaited_once_with(
            {"_id": capture.STATE_ID}, {"$set": {"enabled": False}}, upsert=True,
        )

    async def test_missing_catalog_is_rejected_before_saving(self):
        db = self.make_db()
        db.get_custom_catalog.return_value = None
        with self.assertRaises(ValueError):
            await capture.start_session(db, {"catalog_ids": ["missing"]})
        db.dbs["tracking"]["state"].replace_one.assert_not_awaited()

    async def test_exclusive_catalog_cannot_be_combined(self):
        db = self.make_db()
        db.get_custom_catalog.side_effect = [{"exclusive": True}, {"exclusive": False}]
        with self.assertRaises(ValueError):
            await capture.start_session(db, {"catalog_ids": ["one", "two"]})
        db.dbs["tracking"]["state"].replace_one.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
