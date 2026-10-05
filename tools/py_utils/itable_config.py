"""Shared ITables display settings for Ampere notebooks."""

from __future__ import annotations


def configure_itables() -> None:
    """Enable compact tables with global search and per-column filters."""
    import itables

    itables.init_notebook_mode(all_interactive=False)
    itables.options.column_filters = "header"
    itables.options.layout = {
        "top1": "searchBuilder",
        "topStart": "pageLength",
        "topEnd": "search",
        "bottomStart": "info",
        "bottomEnd": "paging",
    }
    itables.options.pageLength = 25
    itables.options.scrollX = True
    itables.options.maxBytes = 1_000_000
