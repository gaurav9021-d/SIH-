"""Create the clustering schema.

Revision ID: 20260929_0002
Revises: None
Create Date: 2026-09-29

Tables are created runs-first because clusters and noise reference a run. Cluster
members reference ingestion detections by the logical (detection_id, as_of) pair
with no foreign key, so ingestion partition maintenance stays independent of this
stage.
"""

from alembic import op

revision = "20260929_0002"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    op.execute("CREATE SCHEMA IF NOT EXISTS clustering")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS clustering.runs (
          run_id             uuid        PRIMARY KEY,
          status             text        NOT NULL
                             CHECK (status IN ('running', 'succeeded', 'failed', 'skipped')),
          aoi                text        NOT NULL,
          window_start       timestamptz NOT NULL,
          window_end         timestamptz NOT NULL,
          evaluation_time    timestamptz NOT NULL,
          settled_horizon    timestamptz NOT NULL,
          params             jsonb       NOT NULL,
          params_fingerprint text        NOT NULL,
          algorithm_version  text        NOT NULL,
          products           text[]      NOT NULL,
          detections_read    integer     NOT NULL DEFAULT 0 CHECK (detections_read >= 0),
          observations       integer     NOT NULL DEFAULT 0 CHECK (observations >= 0),
          clusters_open      integer     NOT NULL DEFAULT 0 CHECK (clusters_open >= 0),
          clusters_closed    integer     NOT NULL DEFAULT 0 CHECK (clusters_closed >= 0),
          noise_count        integer     NOT NULL DEFAULT 0 CHECK (noise_count >= 0),
          pending_count      integer     NOT NULL DEFAULT 0 CHECK (pending_count >= 0),
          max_extent_m       double precision,
          duration_s         double precision,
          started_at         timestamptz NOT NULL DEFAULT now(),
          finished_at        timestamptz,
          error_summary      text
        )
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS clustering.clusters (
          cluster_id         text        PRIMARY KEY,
          params_fingerprint text        NOT NULL,
          algorithm_version  text        NOT NULL,
          status             text        NOT NULL CHECK (status IN ('open', 'closed')),
          aoi                text        NOT NULL,
          formed_at          timestamptz NOT NULL,
          first_detected     timestamptz NOT NULL,
          last_detected      timestamptz NOT NULL,
          closed_at          timestamptz,
          centroid           geometry(Point, 4326) NOT NULL,
          min_lat            double precision NOT NULL,
          min_lon            double precision NOT NULL,
          max_lat            double precision NOT NULL,
          max_lon            double precision NOT NULL,
          extent_m           double precision NOT NULL,
          detection_count    integer     NOT NULL CHECK (detection_count > 0),
          observation_count  integer     NOT NULL CHECK (observation_count > 0),
          frp_mean           real        NOT NULL,
          frp_var            real        NOT NULL CHECK (frp_var >= 0),
          day_night_ratio    real        NOT NULL CHECK (day_night_ratio BETWEEN 0 AND 1),
          persistence_days   real        NOT NULL CHECK (persistence_days >= 0),
          features_as_of     timestamptz NOT NULL,
          provisional        boolean     NOT NULL DEFAULT false,
          run_id             uuid        NOT NULL REFERENCES clustering.runs(run_id),
          created_at         timestamptz NOT NULL DEFAULT now(),
          updated_at         timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT clusters_extent_non_negative CHECK (extent_m >= 0),
          CONSTRAINT clusters_time_ordered CHECK (first_detected <= last_detected)
        )
        """
    )
    # The 2000 m ceiling is a configured parameter, not a literal, so it cannot be
    # a static CHECK. It is enforced during admission and re-verified for every
    # cluster before persistence, which aborts the run on violation.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_clusters_centroid "
        "ON clustering.clusters USING gist (centroid)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_clusters_scope "
        "ON clustering.clusters (params_fingerprint, status, last_detected)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_clusters_formed_at "
        "ON clustering.clusters (formed_at)"
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS clustering.observations (
          observation_key    text        PRIMARY KEY,
          params_fingerprint text        NOT NULL,
          lat                double precision NOT NULL CHECK (lat BETWEEN -90 AND 90),
          lon                double precision NOT NULL CHECK (lon BETWEEN -180 AND 180),
          geom               geometry(Point, 4326)
                             GENERATED ALWAYS AS
                               (ST_SetSRID(ST_MakePoint(lon, lat), 4326)) STORED,
          as_of              timestamptz NOT NULL,
          frp                real        NOT NULL CHECK (frp >= 0),
          day_night          char(1)     NOT NULL CHECK (day_night IN ('D', 'N')),
          detection_count    integer     NOT NULL CHECK (detection_count > 0),
          products           text[]      NOT NULL,
          satellites         text[]      NOT NULL
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_observations_geom "
        "ON clustering.observations USING gist (geom)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_observations_as_of "
        "ON clustering.observations (as_of)"
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS clustering.cluster_members (
          cluster_id      text        NOT NULL
                          REFERENCES clustering.clusters ON DELETE CASCADE,
          observation_key text        NOT NULL REFERENCES clustering.observations,
          as_of           timestamptz NOT NULL,
          PRIMARY KEY (cluster_id, observation_key)
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_cluster_members_as_of "
        "ON clustering.cluster_members (cluster_id, as_of)"
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS clustering.observation_detections (
          observation_key text        NOT NULL
                          REFERENCES clustering.observations ON DELETE CASCADE,
          detection_id    bigint      NOT NULL,
          detection_as_of timestamptz NOT NULL,
          PRIMARY KEY (observation_key, detection_id, detection_as_of)
        )
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS clustering.noise_observations (
          observation_key    text        PRIMARY KEY
                             REFERENCES clustering.observations,
          params_fingerprint text        NOT NULL,
          as_of              timestamptz NOT NULL,
          settled_at         timestamptz NOT NULL,
          run_id             uuid        NOT NULL REFERENCES clustering.runs(run_id)
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_noise_as_of "
        "ON clustering.noise_observations (as_of)"
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION clustering.clusters_as_of(
          at timestamptz, fingerprint text
        )
        RETURNS TABLE (
          cluster_id text,
          observation_count bigint,
          first_detected timestamptz,
          last_detected timestamptz
        ) AS $$
          SELECT c.cluster_id, count(*), min(m.as_of), max(m.as_of)
          FROM clustering.clusters c
          JOIN clustering.cluster_members m USING (cluster_id)
          WHERE c.params_fingerprint = fingerprint
            AND c.formed_at <= at
            AND m.as_of <= at
          GROUP BY c.cluster_id
        $$ LANGUAGE sql STABLE
        """
    )


def downgrade() -> None:
    op.execute("DROP FUNCTION IF EXISTS clustering.clusters_as_of(timestamptz, text)")
    op.execute("DROP TABLE IF EXISTS clustering.noise_observations")
    op.execute("DROP TABLE IF EXISTS clustering.observation_detections")
    op.execute("DROP TABLE IF EXISTS clustering.cluster_members")
    op.execute("DROP TABLE IF EXISTS clustering.observations")
    op.execute("DROP TABLE IF EXISTS clustering.clusters")
    op.execute("DROP TABLE IF EXISTS clustering.runs")
