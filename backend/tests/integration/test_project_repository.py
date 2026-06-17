import pytest

from backend.db.postgres import ApiKey, Project
from backend.model.auth import get_api_key_prefix, hash_api_key, verify_api_key_hash
from backend.model.config import Settings
from backend.repositories.projects import (
    create_project_api_key,
    get_active_project_by_api_key,
    revoke_project_api_key,
    rotate_project_api_key,
)

pytestmark = pytest.mark.integration


@pytest.fixture
async def test_project(db_session) -> Project:
    project = Project(name="Repository test project", rate_limit_per_minute=42)
    db_session.add(project)
    await db_session.commit()
    await db_session.refresh(project)
    return project


@pytest.mark.anyio
async def test_create_project_api_key_stores_hash_and_prefix(
    db_session,
    test_project: Project,
    test_settings: Settings,
):
    """Creating an API key should persist only hash and safe prefix."""
    api_key, raw_api_key = await create_project_api_key(
        db_session,
        test_project.id,
        "frontend-prod",
        test_settings,
    )

    assert raw_api_key.startswith("ak_live_")
    assert api_key.project_id == test_project.id
    assert api_key.name == "frontend-prod"
    assert api_key.key_prefix == get_api_key_prefix(raw_api_key)
    assert api_key.key_hash != raw_api_key
    assert verify_api_key_hash(
        raw_api_key,
        test_settings.api_key_hash_secret,
        api_key.key_hash,
    )


@pytest.mark.anyio
async def test_get_active_project_by_api_key_returns_matching_project_and_key(
    db_session,
    test_project: Project,
    test_settings: Settings,
):
    """Lookup should return the active project and key for a valid raw API key."""
    api_key, raw_api_key = await create_project_api_key(
        db_session,
        test_project.id,
        "frontend-prod",
        test_settings,
    )

    auth_context = await get_active_project_by_api_key(
        db_session,
        raw_api_key,
        test_settings,
    )

    assert auth_context is not None
    project, stored_api_key = auth_context
    assert project.id == test_project.id
    assert project.rate_limit_per_minute == 42
    assert stored_api_key.id == api_key.id


@pytest.mark.anyio
async def test_get_active_project_by_api_key_ignores_wrong_raw_key(
    db_session,
    test_project: Project,
    test_settings: Settings,
):
    """Lookup should reject raw API keys that do not match the stored hash."""
    await create_project_api_key(
        db_session,
        test_project.id,
        "frontend-prod",
        test_settings,
    )

    auth_context = await get_active_project_by_api_key(
        db_session,
        "ak_live_wrong_key",
        test_settings,
    )

    assert auth_context is None


@pytest.mark.anyio
async def test_revoke_project_api_key_disables_lookup(
    db_session,
    test_project: Project,
    test_settings: Settings,
):
    """Revoking an API key should make it unusable for future auth lookups."""
    api_key, raw_api_key = await create_project_api_key(
        db_session,
        test_project.id,
        "frontend-prod",
        test_settings,
    )

    revoked_api_key = await revoke_project_api_key(
        db_session,
        test_project.id,
        api_key.id,
    )
    auth_context = await get_active_project_by_api_key(
        db_session,
        raw_api_key,
        test_settings,
    )

    assert revoked_api_key is not None
    assert revoked_api_key.is_active is False
    assert revoked_api_key.revoked_at is not None
    assert auth_context is None


@pytest.mark.anyio
async def test_rotate_project_api_key_keeps_old_key_active(
    db_session,
    test_project: Project,
    test_settings: Settings,
):
    """Soft rotation should create a new key without revoking the old key."""
    old_api_key, old_raw_api_key = await create_project_api_key(
        db_session,
        test_project.id,
        "frontend-prod",
        test_settings,
    )

    rotated = await rotate_project_api_key(
        db_session,
        test_project.id,
        old_api_key.id,
        "frontend-prod-v2",
        test_settings,
    )

    assert rotated is not None
    new_api_key, new_raw_api_key = rotated
    assert new_api_key.id != old_api_key.id
    assert new_api_key.name == "frontend-prod-v2"
    assert new_raw_api_key != old_raw_api_key
    assert await get_active_project_by_api_key(db_session, old_raw_api_key, test_settings)
    assert await get_active_project_by_api_key(db_session, new_raw_api_key, test_settings)


@pytest.mark.anyio
async def test_rotate_project_api_key_returns_none_for_inactive_key(
    db_session,
    test_project: Project,
    test_settings: Settings,
):
    """Rotating an inactive key should not create a replacement key."""
    inactive_api_key = ApiKey(
        project_id=test_project.id,
        name="inactive",
        key_hash=hash_api_key("ak_live_inactive", test_settings.api_key_hash_secret),
        key_prefix=get_api_key_prefix("ak_live_inactive"),
        is_active=False,
    )
    db_session.add(inactive_api_key)
    await db_session.commit()
    await db_session.refresh(inactive_api_key)

    rotated = await rotate_project_api_key(
        db_session,
        test_project.id,
        inactive_api_key.id,
        None,
        test_settings,
    )

    assert rotated is None
