"""Seed: Family network and admin user

Revision ID: 0002
Revises: 0001
Create Date: 2026-04-06

"""

import os
from collections.abc import Sequence

import bcrypt
from alembic import op
from sqlalchemy import text

revision: str = "0002"
down_revision: str | Sequence[str] | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    admin_email = os.environ.get("ADMIN_EMAIL", "").strip()
    admin_password = os.environ.get("ADMIN_PASSWORD", "").strip()
    if not admin_email or not admin_password:
        raise RuntimeError(
            "ADMIN_EMAIL and ADMIN_PASSWORD must be set before running seed migration"
        )

    password_hash = bcrypt.hashpw(admin_password.encode(), bcrypt.gensalt()).decode()

    bind = op.get_bind()

    network_id = bind.execute(
        text("INSERT INTO networks (name) VALUES (:name) RETURNING id"),
        {"name": "Family"},
    ).scalar_one()

    admin_id = bind.execute(
        text("""
            INSERT INTO users (email, password_hash, display_name, role)
            VALUES (:email, :password_hash, :display_name, :role)
            RETURNING id
        """),
        {
            "email": admin_email.lower(),
            "password_hash": password_hash,
            "display_name": "Admin",
            "role": "admin",
        },
    ).scalar_one()

    bind.execute(
        text("""
            INSERT INTO network_members (network_id, user_id, role)
            VALUES (:network_id, :user_id, :role)
        """),
        {"network_id": str(network_id), "user_id": str(admin_id), "role": "admin"},
    )


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(
        text(
            "DELETE FROM network_members"
            " WHERE user_id IN (SELECT id FROM users WHERE role = 'admin')"
        )
    )
    bind.execute(
        text("DELETE FROM users WHERE email = :email"),
        {"email": os.environ.get("ADMIN_EMAIL", "").strip().lower()},
    )
    bind.execute(text("DELETE FROM networks WHERE name = 'Family'"))
