"""Shared state for migration pause/resume control."""
_migration_pause_flags: dict[str, bool] = {}
