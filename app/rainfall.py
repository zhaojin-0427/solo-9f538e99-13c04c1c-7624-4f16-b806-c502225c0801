"""Core rainfall ingestion and calculation service.

Design notes
------------

* **Effective observation** -- the active (non-retracted) revision of each
  observation.  Higher revisions win; an identical re-upload is a no-op; a
  different payload at the same revision is a conflict.
* **Uniqueness** -- after merging, a station may have at most one active
  observation per instant.  Instants are compared in UTC at microsecond
  resolution.
* **Calculation** -- active readings are sorted by time.  For each consecutive
  pair, when the later reading is not a reset, the increase is credited to the
  later reading's UTC day.  A reset starts a fresh baseline and contributes no
  rain itself.  The first reading of a segment only defines a baseline.
* **Atomicity** -- the entire batch (including recomputation of daily sums)
  runs in one SQLite transaction.  Any rejection leaves prior observations and
  prior daily results untouched.
"""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from .db import write_transaction
from .errors import ArchiveRejected, BatchRejected, violation
from .models import ObservationIn

UNCHANGED = "unchanged"
CREATED = "created"
UPDATED = "updated"


def canonical_amount(value: float) -> str:
    """Return a stable decimal string for a millimetre amount.

    ``Decimal(repr(float))`` gives the shortest round-trippable form, keeping
    ``12.3`` as ``"12.3"`` and avoiding binary-float artefacts in equality
    checks and arithmetic.
    """

    try:
        decimal = Decimal(repr(float(value)))
    except (InvalidOperation, ValueError) as exc:  # pragma: no cover - guarded upstream
        raise ValueError(f"invalid cumulative_mm: {value!r}") from exc
    if not decimal.is_finite():
        raise ValueError(f"non-finite cumulative_mm: {value!r}")
    return decimal_to_string(decimal)


def decimal_to_string(value: Decimal) -> str:
    """Plain (non-exponent) canonical form for a decimal."""

    normalized = value.normalize()
    if normalized == 0:
        return "0"
    return format(normalized, "f")


def canonical_time(value: datetime) -> str:
    """Canonical UTC timestamp, microsecond precision."""

    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def utc_day(value: datetime) -> date:
    return value.astimezone(timezone.utc).date()


def _row_to_state(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "station_id": row["station_id"],
        "observation_id": row["observation_id"],
        "revision": int(row["revision"]),
        "observed_at": row["observed_at"],
        "observed_day": row["observed_day"],
        "cumulative_mm": row["cumulative_mm"],
        "is_reset": bool(row["is_reset"]),
        "is_retracted": bool(row["is_retracted"]),
    }


def _canonical_payload(observation: ObservationIn) -> tuple:
    return (
        observation.revision,
        canonical_time(observation.observed_at),
        canonical_amount(observation.cumulative_mm),
        observation.is_reset,
        observation.is_retracted,
    )


def _state_payload(row: dict[str, Any]) -> tuple:
    return (
        int(row["revision"]),
        row["observed_at"],
        row["cumulative_mm"],
        bool(row["is_reset"]),
        bool(row["is_retracted"]),
    )


def _fetch_observation(
    conn: sqlite3.Connection, station_id: str, observation_id: str
) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM observations WHERE station_id = ? AND observation_id = ?",
        (station_id, observation_id),
    ).fetchone()
    return _row_to_state(row) if row is not None else None


def _fetch_station_observations(
    conn: sqlite3.Connection, station_id: str
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM observations WHERE station_id = ?", (station_id,)
    ).fetchall()
    return [_row_to_state(row) for row in rows]


def _get_cutoff_day(conn: sqlite3.Connection, station_id: str) -> date | None:
    """Return the station's current UTC archive cutoff day, if archived."""

    row = conn.execute(
        "SELECT cutoff_day FROM archive_cutoffs WHERE station_id = ?",
        (station_id,),
    ).fetchone()
    return date.fromisoformat(row["cutoff_day"]) if row is not None else None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _resolve_batch_duplicates(
    observations: list[ObservationIn],
) -> tuple[list[ObservationIn], list[dict[str, Any]], list[dict[str, Any]]]:
    """Collapse observations repeated in one batch.

    The highest revision wins, so callers may resend a corrected record
    together with the original without caring about ordering.  Two different
    payloads at the same revision are a conflict.  Each superseded duplicate
    is reported as ``unchanged`` so every submitted item has an outcome.
    """

    violations: list[dict[str, Any]] = []
    superseded: list[dict[str, Any]] = []
    groups: dict[tuple[str, str], list[ObservationIn]] = defaultdict(list)
    order: list[tuple[str, str]] = []

    for obs in observations:
        key = (obs.station_id, obs.observation_id)
        if key not in groups:
            order.append(key)
        groups[key].append(obs)

    effective: list[ObservationIn] = []
    for key in order:
        items = groups[key]
        highest = max(item.revision for item in items)
        top = [item for item in items if item.revision == highest]
        top_payloads = {_canonical_payload(item) for item in top}
        if len(top_payloads) > 1:
            violations.append(
                violation(
                    "revision_conflict",
                    "the same revision was uploaded with different content",
                    station_id=key[0],
                    observation_id=key[1],
                    revision=highest,
                )
            )
            continue
        chosen = top[0]
        effective.append(chosen)
        for item in items:
            if item is chosen:
                continue
            superseded.append(
                {
                    "observation_id": item.observation_id,
                    "revision": item.revision,
                    "status": UNCHANGED,
                }
            )

    return effective, superseded, violations


def _enforce_archive_boundaries(
    conn: sqlite3.Connection, observations: Iterable[ObservationIn]
) -> None:
    """Reject a batch that touches archived data.

    Two cases reject the whole batch (including observations for other
    stations):

    * an observation id already lives in the archive -- its revision is frozen
      and the id cannot be re-created or superseded;
    * an observed instant is strictly before the station's cutoff day -- no
      active reading may ever be inserted or rewritten inside archived time.

    Runs inside the batch write transaction, before any row is changed, so a
    rejection leaves no partial state.
    """

    observations = list(observations)
    station_ids = {item.station_id for item in observations}
    cutoffs = {
        station_id: _get_cutoff_day(conn, station_id)
        for station_id in station_ids
    }

    violations: list[dict[str, Any]] = []
    for obs in observations:
        archived = conn.execute(
            "SELECT 1 FROM archived_observations "
            "WHERE station_id = ? AND observation_id = ?",
            (obs.station_id, obs.observation_id),
        ).fetchone()
        if archived is not None:
            violations.append(
                violation(
                    "observation_archived",
                    "the observation has been archived and can no longer be modified",
                    station_id=obs.station_id,
                    observation_id=obs.observation_id,
                )
            )
            continue

        cutoff = cutoffs[obs.station_id]
        if cutoff is not None and utc_day(obs.observed_at) < cutoff:
            violations.append(
                violation(
                    "observed_before_archive_cutoff",
                    "the observed instant is earlier than the station archive cutoff",
                    station_id=obs.station_id,
                    observation_id=obs.observation_id,
                    observed_at=canonical_time(obs.observed_at),
                    cutoff_date=cutoff.isoformat(),
                )
            )

    if violations:
        raise BatchRejected(violations)


def _apply_batch(
    conn: sqlite3.Connection, observations: Iterable[ObservationIn]
) -> tuple[list[dict[str, Any]], set[str]]:
    """Validate and stage the batch inside the caller's transaction.

    Returns per-observation results and the set of station ids whose stored
    state changed (new, updated or replaced).
    """

    violations: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    dirty: set[str] = set()

    effective, superseded, duplicate_violations = _resolve_batch_duplicates(
        list(observations)
    )
    violations.extend(duplicate_violations)
    results.extend(superseded)

    for obs in effective:
        payload = _canonical_payload(obs)
        existing = _fetch_observation(conn, obs.station_id, obs.observation_id)

        if existing is None:
            conn.execute(
                """
                INSERT INTO observations (
                    station_id, observation_id, revision, observed_at,
                    observed_day, cumulative_mm, is_reset, is_retracted, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    obs.station_id,
                    obs.observation_id,
                    obs.revision,
                    payload[1],
                    utc_day(obs.observed_at).isoformat(),
                    payload[2],
                    1 if obs.is_reset else 0,
                    1 if obs.is_retracted else 0,
                    _now(),
                ),
            )
            results.append(
                {
                    "observation_id": obs.observation_id,
                    "revision": obs.revision,
                    "status": CREATED,
                }
            )
            dirty.add(obs.station_id)
            continue

        stored_revision = int(existing["revision"])

        if obs.revision < stored_revision:
            # An older revision than what is stored must not alter anything.
            results.append(
                {
                    "observation_id": obs.observation_id,
                    "revision": obs.revision,
                    "status": UNCHANGED,
                }
            )
            continue

        if obs.revision == stored_revision:
            if _state_payload(existing) == payload:
                # Idempotent re-upload.
                results.append(
                    {
                        "observation_id": obs.observation_id,
                        "revision": obs.revision,
                        "status": UNCHANGED,
                    }
                )
            else:
                violations.append(
                    violation(
                        "revision_conflict",
                        "the same revision was uploaded with different content",
                        station_id=obs.station_id,
                        observation_id=obs.observation_id,
                        revision=obs.revision,
                    )
                )
            continue

        # Higher revision replaces the previous version.
        conn.execute(
            """
            UPDATE observations
               SET revision = ?, observed_at = ?, observed_day = ?,
                   cumulative_mm = ?, is_reset = ?, is_retracted = ?, updated_at = ?
             WHERE station_id = ? AND observation_id = ?
            """,
            (
                payload[0],
                payload[1],
                utc_day(obs.observed_at).isoformat(),
                payload[2],
                1 if obs.is_reset else 0,
                1 if obs.is_retracted else 0,
                _now(),
                obs.station_id,
                obs.observation_id,
            ),
        )
        results.append(
            {
                "observation_id": obs.observation_id,
                "revision": obs.revision,
                "status": UPDATED,
            }
        )
        dirty.add(obs.station_id)

    if violations:
        raise BatchRejected(violations)

    return results, dirty


def _validate_station_state(conn: sqlite3.Connection, station_ids: Iterable[str]) -> None:
    """Check cross-observation invariants after staging the batch.

    Runs after all inserts and updates so that a correction which retracts or
    moves the offending reading can legitimately resolve a previous conflict.
    """

    violations: list[dict[str, Any]] = []

    for station_id in sorted(station_ids):
        active = [
            row
            for row in _fetch_station_observations(conn, station_id)
            if not row["is_retracted"]
        ]
        active.sort(key=lambda row: (row["observed_at"], row["observation_id"]))

        # 1. At most one active observation per instant.
        by_instant: dict[str, list[str]] = defaultdict(list)
        for row in active:
            by_instant[row["observed_at"]].append(row["observation_id"])
        for instant, ids in sorted(by_instant.items()):
            if len(ids) > 1:
                violations.append(
                    violation(
                        "duplicate_active_observation",
                        "two active observations share the same station and instant",
                        station_id=station_id,
                        observed_at=instant,
                        observation_ids=sorted(ids),
                    )
                )

        # 2. Within a reset-to-reset segment the cumulative counter must not
        #    decrease.  A reset establishes a fresh baseline at its own
        #    reading, so any value before it is irrelevant; the reset reading
        #    itself is compared against nothing.  Checking against the highest
        #    reading in the segment (rather than only the predecessor) catches
        #    a reversal that a later reading partly hides.
        segment_high: Decimal | None = None
        for row in active:
            current = Decimal(row["cumulative_mm"])
            if row["is_reset"]:
                segment_high = current
                continue
            if segment_high is not None and current < segment_high:
                violations.append(
                    violation(
                        "cumulative_regression_without_reset",
                        "cumulative rainfall decreased without a reset flag",
                        station_id=station_id,
                        observed_at=row["observed_at"],
                        observation_id=row["observation_id"],
                        cumulative_mm=row["cumulative_mm"],
                        previous_cumulative_mm=decimal_to_string(segment_high),
                    )
                )
            if segment_high is None or current > segment_high:
                segment_high = current

    if violations:
        raise BatchRejected(violations)


def _calculate_station(
    observations: list[dict[str, Any]],
) -> dict[date, tuple[Decimal, list[str]]]:
    """Compute daily rainfall totals for one station.

    Returns a mapping of UTC day -> (total in mm, contributing observation
    ids).  A reading contributes to a day when the difference between it and
    the preceding reading in the same reset-free segment is credited to that
    day.  The very first reading after a reset only establishes a baseline and
    therefore contributes nowhere.
    """

    active = [row for row in observations if not row["is_retracted"]]
    active.sort(key=lambda row: (row["observed_at"], row["observation_id"]))

    daily: dict[date, tuple[Decimal, list[str]]] = {}
    previous: dict[str, Any] | None = None

    for row in active:
        if row["is_reset"]:
            # The reset reading is the new baseline: it contributes no
            # incremental rain, but the next reading is measured from it.
            previous = row
            continue
        if previous is not None:
            amount = Decimal(row["cumulative_mm"]) - Decimal(previous["cumulative_mm"])
            if amount != 0:
                day = date.fromisoformat(row["observed_day"])
                total, ids = daily.get(day, (Decimal("0"), []))
                daily[day] = (
                    total + amount,
                    [*ids, row["observation_id"]],
                )
        previous = row

    return {
        day: (total, sorted(set(ids))) for day, (total, ids) in daily.items()
    }


def _refresh_daily_results(conn: sqlite3.Connection, dirty: set[str]) -> set[date]:
    """Recompute and persist daily rows for every affected station.

    A station-wide recompute is cheap compared with correctness: a late,
    corrected or retracted observation can change the total of its own day and
    of the next day, and a reset can move segment boundaries.  Recomputing per
    affected station keeps all of those cases consistent in one pass.  Days
    that no longer have any contributing reading are deleted.
    """

    affected_days: set[date] = set()

    for station_id in sorted(dirty):
        # Daily results strictly before the archive cutoff belong to the
        # archived period and must remain byte-for-byte identical: the active
        # set no longer contains the readings that produced them, so they are
        # neither recomputed nor deleted here.
        cutoff_day = _get_cutoff_day(conn, station_id)
        observations = _fetch_station_observations(conn, station_id)
        results = _calculate_station(observations)

        existing = {
            date.fromisoformat(row["day"]): (
                row["rainfall_mm"],
                json.loads(row["observation_ids"]),
            )
            for row in conn.execute(
                "SELECT day, rainfall_mm, observation_ids FROM daily_rainfall "
                "WHERE station_id = ?",
                (station_id,),
            ).fetchall()
        }

        for day in set(existing) | set(results):
            if cutoff_day is not None and day < cutoff_day:
                continue
            if day in results:
                total, ids = results[day]
                canonical_total = decimal_to_string(total)
                stored = (canonical_total, sorted(set(ids)))
                if existing.get(day) == stored:
                    continue
                conn.execute(
                    """
                    INSERT INTO daily_rainfall (
                        station_id, day, rainfall_mm, observation_ids, updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (station_id, day) DO UPDATE SET
                        rainfall_mm = excluded.rainfall_mm,
                        observation_ids = excluded.observation_ids,
                        updated_at = excluded.updated_at
                    """,
                    (
                        station_id,
                        day.isoformat(),
                        canonical_total,
                        json.dumps(stored[1], separators=(",", ":")),
                        _now(),
                    ),
                )
                affected_days.add(day)
            elif day in existing:
                conn.execute(
                    "DELETE FROM daily_rainfall WHERE station_id = ? AND day = ?",
                    (station_id, day.isoformat()),
                )
                affected_days.add(day)

    return affected_days


def ingest_batch(
    conn: sqlite3.Connection, observations: Iterable[ObservationIn]
) -> dict[str, Any]:
    """Apply an upload batch transactionally and return its outcome."""

    observations = list(observations)
    with write_transaction(conn):
        # Boundary checks precede every write so an offending batch (including
        # its valid observations for other stations) is rejected wholesale.
        _enforce_archive_boundaries(conn, observations)
        results, dirty = _apply_batch(conn, observations)
        _validate_station_state(conn, dirty)
        affected_days = _refresh_daily_results(conn, dirty)

    return {
        "accepted": len(results),
        "created": sum(1 for item in results if item["status"] == CREATED),
        "updated": sum(1 for item in results if item["status"] == UPDATED),
        "unchanged": sum(1 for item in results if item["status"] == UNCHANGED),
        "results": results,
        "affected_dates": sorted(affected_days),
    }


def get_daily_rainfall(
    conn: sqlite3.Connection, station_id: str, day: date
) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM daily_rainfall WHERE station_id = ? AND day = ?",
        (station_id, day.isoformat()),
    ).fetchone()
    if row is None:
        return None
    return {
        "station_id": row["station_id"],
        "date": date.fromisoformat(row["day"]),
        "rainfall_mm": float(Decimal(row["rainfall_mm"])),
        "observation_ids": json.loads(row["observation_ids"]),
    }


def list_station_observations(
    conn: sqlite3.Connection, station_id: str
) -> list[dict[str, Any]]:
    """Return every observation for a station, active and archived.

    Archived rows carry ``archived=True`` and their archival timestamp so the
    historical trail stays queryable through the same endpoint as active data.
    """

    rows = conn.execute(
        """
        SELECT station_id, observation_id, revision, observed_at,
               observed_day, cumulative_mm, is_reset, is_retracted,
               updated_at, NULL AS archived_at
          FROM observations
         WHERE station_id = ?
        UNION ALL
        SELECT station_id, observation_id, revision, observed_at,
               observed_day, cumulative_mm, is_reset, is_retracted,
               updated_at, archived_at
          FROM archived_observations
         WHERE station_id = ?
         ORDER BY observed_at, observation_id
        """,
        (station_id, station_id),
    ).fetchall()
    return [
        {
            "observation_id": row["observation_id"],
            "station_id": row["station_id"],
            "revision": int(row["revision"]),
            "observed_at": datetime.fromisoformat(row["observed_at"]),
            "cumulative_mm": float(Decimal(row["cumulative_mm"])),
            "is_reset": bool(row["is_reset"]),
            "is_retracted": bool(row["is_retracted"]),
            "archived": row["archived_at"] is not None,
            "archived_at": (
                datetime.fromisoformat(row["archived_at"])
                if row["archived_at"] is not None
                else None
            ),
        }
        for row in rows
    ]


def archive_station(
    conn: sqlite3.Connection, station_id: str, cutoff_day: date
) -> dict[str, Any]:
    """Move a station's pre-cutoff observations into the archive.

    The last non-retracted reading before the cutoff stays in the active set
    as the baseline for post-cutoff differences; every other active row whose
    UTC day is strictly before the cutoff is copied to ``archived_observations``
    and deleted.  Daily results are left untouched -- they already cover the
    archived period and the boundary guard keeps them frozen.

    The cutoff only moves forward.  Everything happens in one immediate
    transaction, so a failure leaves neither a partial migration nor a changed
    cutoff.
    """

    with write_transaction(conn):
        previous = _get_cutoff_day(conn, station_id)
        if previous is not None and cutoff_day <= previous:
            raise ArchiveRejected(
                [
                    violation(
                        "cutoff_not_advanced",
                        "the archive cutoff day can only move forward",
                        station_id=station_id,
                        cutoff_date=cutoff_day.isoformat(),
                        previous_cutoff_date=previous.isoformat(),
                    )
                ]
            )

        now = _now()

        # Anchor: the latest pre-cutoff non-retracted reading.  Keeping it
        # active preserves the consecutive-pair difference rule across the
        # boundary, including reset baselines.
        anchor_row = conn.execute(
            """
            SELECT * FROM observations
             WHERE station_id = ?
               AND observed_day < ?
               AND is_retracted = 0
             ORDER BY observed_at DESC, observation_id DESC
             LIMIT 1
            """,
            (station_id, cutoff_day.isoformat()),
        ).fetchone()
        anchor_id = anchor_row["observation_id"] if anchor_row is not None else None

        archived_rows = conn.execute(
            """
            SELECT * FROM observations
             WHERE station_id = ?
               AND observed_day < ?
               AND (? IS NULL OR observation_id != ?)
             ORDER BY observed_at, observation_id
            """,
            (station_id, cutoff_day.isoformat(), anchor_id, anchor_id),
        ).fetchall()

        for row in archived_rows:
            conn.execute(
                """
                INSERT INTO archived_observations (
                    station_id, observation_id, revision, observed_at,
                    observed_day, cumulative_mm, is_reset, is_retracted,
                    updated_at, archived_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["station_id"],
                    row["observation_id"],
                    row["revision"],
                    row["observed_at"],
                    row["observed_day"],
                    row["cumulative_mm"],
                    row["is_reset"],
                    row["is_retracted"],
                    row["updated_at"],
                    now,
                ),
            )
            conn.execute(
                "DELETE FROM observations "
                "WHERE station_id = ? AND observation_id = ?",
                (row["station_id"], row["observation_id"]),
            )

        conn.execute(
            """
            INSERT INTO archive_cutoffs (station_id, cutoff_day, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT (station_id) DO UPDATE SET
                cutoff_day = excluded.cutoff_day,
                updated_at = excluded.updated_at
            """,
            (station_id, cutoff_day.isoformat(), now),
        )

    return {
        "station_id": station_id,
        "cutoff_date": cutoff_day,
        "previous_cutoff_date": previous,
        "archived": len(archived_rows),
        "retained_observation_id": anchor_id,
        "updated_at": datetime.fromisoformat(now),
    }


def get_archive_status(
    conn: sqlite3.Connection, station_id: str
) -> dict[str, Any]:
    """Return a station's cutoff and the counts behind it."""

    cutoff = _get_cutoff_day(conn, station_id)
    cutoff_row = conn.execute(
        "SELECT updated_at FROM archive_cutoffs WHERE station_id = ?",
        (station_id,),
    ).fetchone()
    archived_count = conn.execute(
        "SELECT COUNT(*) AS n FROM archived_observations WHERE station_id = ?",
        (station_id,),
    ).fetchone()["n"]

    anchor_id = None
    if cutoff is not None:
        anchor_row = conn.execute(
            """
            SELECT observation_id FROM observations
             WHERE station_id = ?
               AND observed_day < ?
               AND is_retracted = 0
             ORDER BY observed_at DESC, observation_id DESC
             LIMIT 1
            """,
            (station_id, cutoff.isoformat()),
        ).fetchone()
        anchor_id = anchor_row["observation_id"] if anchor_row is not None else None

    return {
        "station_id": station_id,
        "cutoff_date": cutoff,
        "archived_count": archived_count,
        "retained_observation_id": anchor_id,
        "updated_at": (
            datetime.fromisoformat(cutoff_row["updated_at"])
            if cutoff_row is not None
            else None
        ),
    }
