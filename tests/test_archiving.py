"""End-to-end tests for station observation archiving."""

from __future__ import annotations

import threading
from datetime import date

from conftest import daily, obs, upload


def archive(client, station_id: str, cutoff_date: str):
    return client.post(
        f"/stations/{station_id}/archive",
        json={"cutoff_date": cutoff_date},
    )


def archive_status(client, station_id: str):
    return client.get(f"/stations/{station_id}/archive")


def _ids(rows, *, archived: bool):
    return [
        row["observation_id"]
        for row in rows
        if row["archived"] is archived
    ]


def test_archive_moves_old_rows_but_keeps_last_reading_as_baseline(client):
    upload(
        client,
        obs("O1", "2026-11-01T00:00:00Z", 0.0),
        obs("O2", "2026-11-01T12:00:00Z", 5.0),
        obs("O3", "2026-11-02T00:00:00Z", 9.0),
    )
    before_1101 = daily(client, day="2026-11-01").json()
    before_1102 = daily(client, day="2026-11-02").json()

    response = archive(client, "S1", "2026-11-02")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["station_id"] == "S1"
    assert body["cutoff_date"] == "2026-11-02"
    assert body["previous_cutoff_date"] is None
    assert body["archived"] == 1
    # O2 is the last non-retracted pre-cutoff reading and stays active.
    assert body["retained_observation_id"] == "O2"
    assert body["updated_at"]

    rows = client.get("/stations/S1/observations").json()["observations"]
    assert _ids(rows, archived=False) == ["O2", "O3"]
    archived_rows = [row for row in rows if row["archived"]]
    assert [row["observation_id"] for row in archived_rows] == ["O1"]
    assert archived_rows[0]["archived_at"] is not None
    # The archived record keeps its full payload for traceability.
    assert archived_rows[0]["cumulative_mm"] == 0.0
    assert archived_rows[0]["revision"] == 1

    # Daily results are preserved exactly, including the day whose
    # contributing pair no longer exists in the active set.
    assert daily(client, day="2026-11-01").json() == before_1101
    assert daily(client, day="2026-11-02").json() == before_1102


def test_post_cutoff_differences_use_retained_baseline(client):
    upload(
        client,
        obs("O1", "2026-11-01T00:00:00Z", 0.0),
        obs("O2", "2026-11-01T12:00:00Z", 5.0),
        obs("O3", "2026-11-02T00:00:00Z", 9.0),
    )
    archive(client, "S1", "2026-11-02")

    # A new reading is measured from the retained anchor O2 (5.0 mm).
    response = upload(client, obs("O4", "2026-11-02T12:00:00Z", 12.0))
    assert response.status_code == 200, response.text

    day = daily(client, day="2026-11-02").json()
    # 9 - 5 (O3) + 12 - 9 (O4) = 7.
    assert day["rainfall_mm"] == 7.0
    assert day["observation_ids"] == ["O3", "O4"]
    # The frozen archived day is untouched.
    assert daily(client, day="2026-11-01").json()["rainfall_mm"] == 5.0


def test_reset_reading_can_be_the_retained_baseline(client):
    upload(
        client,
        obs("O1", "2026-11-01T00:00:00Z", 100.0),
        obs("R1", "2026-11-01T12:00:00Z", 2.0, is_reset=True),
        obs("O2", "2026-11-02T00:00:00Z", 5.0),
    )
    response = archive(client, "S1", "2026-11-02")
    assert response.status_code == 200
    assert response.json()["retained_observation_id"] == "R1"
    assert response.json()["archived"] == 1

    # Reset semantics survive the boundary: differences restart from R1.
    assert upload(client, obs("O3", "2026-11-02T12:00:00Z", 6.0)).status_code == 200
    day = daily(client, day="2026-11-02").json()
    # 5 - 2 (O2) + 6 - 5 (O3) = 4.
    assert day["rainfall_mm"] == 4.0
    assert day["observation_ids"] == ["O2", "O3"]


def test_upload_before_cutoff_rejects_whole_batch_including_other_stations(client):
    upload(
        client,
        obs("O1", "2026-11-01T00:00:00Z", 0.0),
        obs("O2", "2026-11-01T12:00:00Z", 5.0),
    )
    archive(client, "S1", "2026-11-02")

    response = upload(
        client,
        # Offending row for the archived station.
        obs("OLD", "2026-11-01T18:00:00Z", 6.0, station_id="S1"),
        # Perfectly valid row for another station: must not take effect either.
        obs("B1", "2026-11-02T00:00:00Z", 1.0, station_id="S2"),
    )
    assert response.status_code == 409
    details = response.json()["error"]["details"]
    assert details[0]["code"] == "observed_before_archive_cutoff"
    assert details[0]["station_id"] == "S1"
    assert details[0]["cutoff_date"] == "2026-11-02"

    assert client.get("/stations/S2/observations").json()["observations"] == []
    rows = client.get("/stations/S1/observations").json()["observations"]
    assert _ids(rows, archived=False) == ["O2"]


def test_upload_touching_archived_observation_id_is_rejected(client):
    upload(
        client,
        obs("O1", "2026-11-01T00:00:00Z", 0.0),
        obs("O2", "2026-11-01T12:00:00Z", 5.0),
    )
    archive(client, "S1", "2026-11-02")

    # Even a higher revision cannot resurrect an archived id.
    response = upload(
        client,
        obs("O1", "2026-11-03T00:00:00Z", 9.0, revision=2),
        obs("B1", "2026-11-03T00:00:00Z", 1.0, station_id="S2"),
    )
    assert response.status_code == 409
    details = response.json()["error"]["details"]
    assert details[0]["code"] == "observation_archived"
    assert details[0]["observation_id"] == "O1"
    assert client.get("/stations/S2/observations").json()["observations"] == []


def test_identical_reupload_of_anchor_is_rejected(client):
    upload(
        client,
        obs("O1", "2026-11-01T00:00:00Z", 0.0),
        obs("O2", "2026-11-01T12:00:00Z", 5.0),
    )
    archive(client, "S1", "2026-11-02")

    # The anchor's instant is still before the cutoff, so no writes are allowed.
    response = upload(client, obs("O2", "2026-11-01T12:00:00Z", 5.0))
    assert response.status_code == 409
    assert response.json()["error"]["details"][0]["code"] == (
        "observed_before_archive_cutoff"
    )


def test_upload_on_cutoff_day_is_accepted(client):
    upload(
        client,
        obs("O1", "2026-11-01T00:00:00Z", 0.0),
        obs("O2", "2026-11-01T12:00:00Z", 5.0),
    )
    archive(client, "S1", "2026-11-02")

    response = upload(client, obs("O3", "2026-11-02T00:00:00Z", 8.0))
    assert response.status_code == 200, response.text
    assert daily(client, day="2026-11-02").json() == {
        "station_id": "S1",
        "date": "2026-11-02",
        "rainfall_mm": 3.0,
        "observation_ids": ["O3"],
    }


def test_cutoff_can_only_move_forward(client):
    upload(client, obs("O1", "2026-11-01T00:00:00Z", 0.0))
    assert archive(client, "S1", "2026-11-02").status_code == 200

    same = archive(client, "S1", "2026-11-02")
    assert same.status_code == 409
    detail = same.json()["error"]["details"][0]
    assert detail["code"] == "cutoff_not_advanced"
    assert detail["previous_cutoff_date"] == "2026-11-02"

    earlier = archive(client, "S1", "2026-10-31")
    assert earlier.status_code == 409
    # The failed attempt moved nothing.
    status = archive_status(client, "S1").json()
    assert status["cutoff_date"] == "2026-11-02"
    assert status["archived_count"] == 0  # O1 was retained as the anchor


def test_advancing_cutoff_archives_the_previous_anchor(client):
    upload(
        client,
        obs("O1", "2026-11-01T00:00:00Z", 0.0),
        obs("O2", "2026-11-01T12:00:00Z", 5.0),
        obs("O3", "2026-11-02T00:00:00Z", 9.0),
    )
    first = archive(client, "S1", "2026-11-02").json()
    assert first["archived"] == 1
    assert first["retained_observation_id"] == "O2"

    second = archive(client, "S1", "2026-11-03")
    assert second.status_code == 200, second.text
    body = second.json()
    assert body["previous_cutoff_date"] == "2026-11-02"
    # O1 is already archived; O2 now yields its anchor role to O3.
    assert body["archived"] == 1
    assert body["retained_observation_id"] == "O3"

    rows = client.get("/stations/S1/observations").json()["observations"]
    assert _ids(rows, archived=False) == ["O3"]
    assert sorted(_ids(rows, archived=True)) == ["O1", "O2"]


def test_retracted_pre_cutoff_rows_are_archived_without_anchor(client):
    upload(
        client,
        obs("GONE", "2026-11-01T00:00:00Z", 0.0, is_retracted=True),
        obs("O2", "2026-11-02T00:00:00Z", 5.0),
    )
    response = archive(client, "S1", "2026-11-02")
    assert response.status_code == 200
    assert response.json()["archived"] == 1
    assert response.json()["retained_observation_id"] is None

    # With no anchor, the first post-cutoff reading only establishes a baseline.
    assert upload(client, obs("O3", "2026-11-02T12:00:00Z", 7.0)).status_code == 200
    assert daily(client, day="2026-11-02").json()["rainfall_mm"] == 2.0


def test_archive_of_unknown_station_sets_boundary_without_rows(client):
    response = archive(client, "S99", "2026-11-02")
    assert response.status_code == 200
    assert response.json()["archived"] == 0
    assert response.json()["retained_observation_id"] is None

    # The boundary is enforced even though nothing was moved.
    rejected = upload(
        client, obs("OLD", "2026-11-01T00:00:00Z", 1.0, station_id="S99")
    )
    assert rejected.status_code == 409
    accepted = upload(
        client, obs("NEW", "2026-11-02T00:00:00Z", 1.0, station_id="S99")
    )
    assert accepted.status_code == 200


def test_archive_does_not_touch_other_stations(client):
    upload(
        client,
        obs("A1", "2026-11-01T00:00:00Z", 0.0, station_id="S1"),
        obs("A2", "2026-11-01T12:00:00Z", 4.0, station_id="S1"),
        obs("B1", "2026-11-01T00:00:00Z", 0.0, station_id="S2"),
        obs("B2", "2026-11-01T12:00:00Z", 6.0, station_id="S2"),
    )
    assert archive(client, "S1", "2026-11-02").status_code == 200

    s2_rows = client.get("/stations/S2/observations").json()["observations"]
    assert _ids(s2_rows, archived=False) == ["B1", "B2"]
    assert _ids(s2_rows, archived=True) == []
    assert archive_status(client, "S2").json()["cutoff_date"] is None
    assert daily(client, station_id="S2", day="2026-11-01").json()[
        "rainfall_mm"
    ] == 6.0


def test_archive_status_endpoint(client):
    before = archive_status(client, "S1")
    assert before.status_code == 200
    assert before.json() == {
        "station_id": "S1",
        "cutoff_date": None,
        "archived_count": 0,
        "retained_observation_id": None,
        "updated_at": None,
    }

    upload(
        client,
        obs("O1", "2026-11-01T00:00:00Z", 0.0),
        obs("O2", "2026-11-01T12:00:00Z", 5.0),
    )
    archive(client, "S1", "2026-11-02")

    after = archive_status(client, "S1").json()
    assert after["cutoff_date"] == "2026-11-02"
    assert after["archived_count"] == 1
    assert after["retained_observation_id"] == "O2"
    assert after["updated_at"]


def test_archive_accepts_camel_case_alias(client):
    upload(client, obs("O1", "2026-11-01T00:00:00Z", 0.0))
    response = client.post(
        "/stations/S1/archive", json={"cutoffDate": "2026-11-02"}
    )
    assert response.status_code == 200
    assert response.json()["cutoff_date"] == "2026-11-02"


def test_archive_validates_payload(client):
    bad_date = client.post(
        "/stations/S1/archive", json={"cutoff_date": "not-a-date"}
    )
    assert bad_date.status_code == 422

    missing = client.post("/stations/S1/archive", json={})
    assert missing.status_code == 422

    extra = client.post(
        "/stations/S1/archive",
        json={"cutoff_date": "2026-11-02", "unexpected": True},
    )
    assert extra.status_code == 422


def test_failed_archive_leaves_everything_in_place(client):
    upload(
        client,
        obs("O1", "2026-11-01T00:00:00Z", 0.0),
        obs("O2", "2026-11-01T12:00:00Z", 5.0),
    )
    archive(client, "S1", "2026-11-02")
    before_rows = client.get("/stations/S1/observations").json()
    before_day = daily(client, day="2026-11-01").json()

    failed = archive(client, "S1", "2026-11-01")
    assert failed.status_code == 409
    assert client.get("/stations/S1/observations").json() == before_rows
    assert daily(client, day="2026-11-01").json() == before_day


def test_concurrent_archive_and_upload_are_serialized_by_commit_order(tmp_path):
    """Archive and upload take the same write lock; one commit order wins.

    Repeated runs cover both orderings.  Whichever happens, the database must
    stay consistent with no partial migration.
    """

    from app.db import Database
    from app.models import ObservationIn
    from app.rainfall import archive_station, get_archive_status, ingest_batch

    for _ in range(10):
        db = Database(str(tmp_path / "conc.db"))
        db.initialize()
        with db.connection() as conn:
            ingest_batch(
                conn,
                [
                    ObservationIn.model_validate(
                        obs("O1", "2026-11-01T00:00:00Z", 0.0)
                    ),
                    ObservationIn.model_validate(
                        obs("O2", "2026-11-01T12:00:00Z", 5.0)
                    ),
                ],
            )

        barrier = threading.Barrier(2)
        outcomes: dict[str, object] = {}

        def run_archive(
            database: Database,
            sync: threading.Barrier,
            result: dict[str, object],
        ) -> None:
            sync.wait()
            try:
                with database.connection() as conn:
                    result["archive"] = archive_station(
                        conn, "S1", date(2026, 11, 2)
                    )
            except Exception as exc:  # noqa: BLE001 - recorded for assertions
                result["archive_error"] = exc

        def run_upload(
            database: Database,
            sync: threading.Barrier,
            result: dict[str, object],
        ) -> None:
            sync.wait()
            try:
                with database.connection() as conn:
                    result["upload"] = ingest_batch(
                        conn,
                        [
                            ObservationIn.model_validate(
                                obs("LATE", "2026-11-01T18:00:00Z", 7.0)
                            )
                        ],
                    )
            except Exception as exc:  # noqa: BLE001
                result["upload_error"] = exc

        threads = [
            threading.Thread(target=run_archive, args=(db, barrier, outcomes)),
            threading.Thread(target=run_upload, args=(db, barrier, outcomes)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert "archive_error" not in outcomes
        with db.connection() as conn:
            status = get_archive_status(conn, "S1")
            rows = list_station_snapshot(conn)

        all_ids = sorted(rows)
        if "upload_error" in outcomes:
            # Archive committed first: the pre-cutoff upload is rejected.
            assert status["archived_count"] == 1
            assert status["retained_observation_id"] == "O2"
            assert all_ids == ["O1", "O2"]
        else:
            # Upload committed first: the archive then retains LATE as the
            # newest pre-cutoff reading and archives both older rows.
            assert outcomes["upload"]["created"] == 1
            assert status["archived_count"] == 2
            assert status["retained_observation_id"] == "LATE"
            assert all_ids == ["LATE", "O1", "O2"]
        db.close()
        tmp_path.joinpath("conc.db").unlink(missing_ok=True)


def list_station_snapshot(conn) -> list[str]:
    active = {
        row["observation_id"]
        for row in conn.execute(
            "SELECT observation_id FROM observations WHERE station_id = 'S1'"
        ).fetchall()
    }
    archived = {
        row["observation_id"]
        for row in conn.execute(
            "SELECT observation_id FROM archived_observations WHERE station_id = 'S1'"
        ).fetchall()
    }
    # Every id lives in exactly one place: no partial migration.
    assert not (active & archived)
    return sorted(active | archived)
